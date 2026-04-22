#!/usr/bin/env python3
"""
BluRay Ripper - Strongly opinionated media extraction pipeline.
Detects BluRay disks, rips with MakeMKV, encodes with HandBrake, and organizes into Plex-compatible structure.
Supports both movies and TV shows with auto-detection.

Requirements:
  pip install requests pyyaml
  System: makemkvcon, handbrake-cli
"""

import os
import sys
import subprocess
import argparse
import logging
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple
import requests
import re

# ============================================================================
# CONFIG: STRONGLY OPINIONATED DEFAULTS
# ============================================================================

# Default configuration for movies (maintain backward compatibility)
DEFAULT_CONFIG = {
    # BluRay device (override with --device)
    "device": "/dev/sr0",

    # Output directory structure (Plex-compatible)
    "output_root": os.path.expanduser("~/Media/Plex/Movies"),
    "output_root_tv_shows": os.path.expanduser("~/Media/Plex/TV Shows"),

    # Temporary scratch space (relative to script location by default)
    "scratch_dir": Path(__file__).parent / "tmp" / "bluray_scratch",

    # OMDb API key (get from https://www.omdbapi.com/apikey.aspx)
    "omdb_api_key": os.environ.get("OMDB_API_KEY", ""),

    # HandBrake defaults (strongly opinionated for modern streaming)
    "handbrake": {
        "preset": "Fast 1080p30",  # or: "Universal", "Fast 720p30", "Super HQ 1080p30"
        "format": "mkv",
        "video_codec": "hevc",  # h.265 for filesize
        "audio_codec": "aac",
        "audio_bitrate": "128",
        "audio_language": "eng",  # primary audio language
        "quality": 22,  # 0=lossless, 28=low quality; 22 is good balance
        "use_gpu": True,  # Enable NVIDIA NVENC if available
        "gpu_device": 0,  # GPU device ID (0 for first GPU)
    },

    # MakeMKV defaults
    "makemkv": {
        "use_largest_title": True,  # Auto-select longest title (usually the feature)
        "min_duration_seconds": 600,  # Skip titles shorter than 10 minutes
    },

    # TV Show detection settings
    "tv_shows": {
        "auto_detect": True,  # Auto-detect TV show vs movie from disk metadata
        "min_title_duration_minutes": 10,  # Minimum title duration for consideration
        "seasonal_output": True,  # Organize TV shows by season
    },

    # Logging
    "log_level": "INFO",
    "log_file": os.path.expanduser("~/logs/bluray_ripper.log"),
}


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load configuration from YAML file or use defaults.
    Falls back to DEFAULT_CONFIG if no config file found.
    """
    if config_path is None:
        # Look for config file next to this script
        script_dir = Path(__file__).parent.resolve()
        print(script_dir)
        config_paths = [
            script_dir / "config" / "defaults.yaml",
            script_dir / "defaults.yaml",
        ]
        config_path = next((p for p in config_paths if p.exists()), None)

    if config_path and Path(config_path).exists():
        try:
            import yaml
            with open(config_path, 'r') as f:
                loaded_config = yaml.safe_load(f) or {}

                # Merge loaded config with defaults (loaded takes precedence)
                merged_config = {**DEFAULT_CONFIG}
                for key, value in loaded_config.items():
                    if isinstance(value, dict) and key in DEFAULT_CONFIG and isinstance(DEFAULT_CONFIG[key], dict):
                        merged_config[key] = {**DEFAULT_CONFIG[key], **value}
                    else:
                        merged_config[key] = value

            # Add debug logging to show what was loaded (using module-level logging)
            for key, value in merged_config.items():
                if key == "handbrake":
                    continue  # Skip nested dicts
            return merged_config

        except ImportError:
            pass  # Use defaults
        except Exception:
            pass  # Use defaults

    return DEFAULT_CONFIG.copy()


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class MediaInfo:
    """Metadata from OMDb."""
    title: str
    year: int
    imdb_id: str
    plot: str
    rating: str

    def plex_folder_name(self) -> str:
        """Return Plex-compatible folder name: 'Title (Year)'"""
        return f"{self.title} ({self.year})"


@dataclass
class TitleInfo:
    """Information about a detected title on the disk."""
    name: str
    duration_minutes: float
    title_id: int = 0


# ============================================================================
# TV SHOW DETECTION & PLEX FOLDER STRUCTURE
# ============================================================================

def scan_disk_for_titles(device: str, logger: logging.Logger) -> List[TitleInfo]:
    """
    Scan BluRay disk for all titles and return their durations.
    Uses bdmt_eng.xml metadata if available, otherwise parses makemkv output.

    Returns list of TitleInfo objects sorted by duration (longest first).
    """
    import xml.etree.ElementTree as ET

    mount_point = None
    we_mounted = False
    titles: List[TitleInfo] = []

    try:
        # Check if device is already mounted
        result = subprocess.run(
            ["findmnt", "-n", "-o", "TARGET", device],
            capture_output=True,
            text=True,
            timeout=5,
        )

        if result.returncode == 0:
            mount_point = result.stdout.strip()
            we_mounted = False
        else:
            mount_point = "/mnt/bluray_detect"
            if not mount_device(device, mount_point, logger):
                return titles
            we_mounted = True

        # Try to read metadata XML for title info
        xml_file = Path(mount_point) / "BDMV" / "META" / "DL" / "bdmt_eng.xml"

        if xml_file.exists():
            try:
                tree = ET.parse(xml_file)
                root = tree.getroot()

                ns = {'di': 'urn:BDA:bdmv;discinfo'}

                # Get all title/episodenames and their durations
                for title_elem in root.findall(".//di:title/di:name", ns):
                    if title_elem.text:
                        titles.append(TitleInfo(name=title_elem.text.strip(), duration_minutes=0))

                if titles:
                    logger.info(f"Found {len(titles)} titles from metadata")
            except ET.ParseError:
                pass

        return titles

    except Exception as e:
        logger.error(f"Error scanning disk for titles: {e}")
        return titles
    finally:
        if we_mounted and mount_point:
            try:
                subprocess.run(
                    ["sudo", "umount", mount_point],
                    capture_output=True,
                    timeout=5,
                )
            except Exception:
                pass


def get_makemkv_title_list(device: str, min_duration: int = 600, logger: Optional[logging.Logger] = None) -> List[TitleInfo]:
    """
    Get list of titles from MakeMKV info command.
    Parses makemkvcon output for title IDs.

    Output format:
    - File 00705.mpls was added as title #0 (title passes duration filter)
    - Title #xxx.mpls has length of X seconds... (skipped due to --minlength)

    Since MakeMKV doesn't report durations for passed titles, we extract all
    "added as title" entries and use placeholder durations. The actual episode
    metadata comes from parsing the title names later.

    Returns list of TitleInfo objects sorted by duration (longest first).
    """
    import logging as logging_module
    if logger is None:
        logger = logging_module.getLogger(__name__)

    titles: List[TitleInfo] = []

    try:
        # Run makemkvcon in info mode to get title info
        # Note: --messages=stdout doesn't work with subprocess, so we use default output
        # Use minlength=0 (very fast) for initial scan, then filter based on duration if needed
        cmd = [
            "makemkvcon",
            "--minlength=0",  # Include all titles to avoid timeout issues
            "info",
            "dev:" + device,
        ]

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        try:
            output, _ = process.communicate(timeout=180)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            logger.error("MakeMKV info timed out")
            return titles

        # Parse makemkvcon info output for title additions
        # Format: "File xxx.mpls was added as title #N"
        current_skipped_duration = None

        for line in output.split('\n'):
            # Track duration from skipped titles (these have length info)
            skipped_match = re.search(r'Title\s+#\d+\.mpls has length of (\d+) seconds', line)
            if skipped_match:
                current_skipped_duration = int(skipped_match.group(1))

            # Check for "File xxx.mpls was added as title #N" pattern
            title_add_match = re.search(r'File\s+\S+\.mpls\s+was\s+added\s+as\s+title\s+#(\d+)', line)
            if title_add_match:
                title_id = int(title_add_match.group(1))
                # Use the last seen skipped duration, or fallback to min_duration + buffer
                duration_seconds = current_skipped_duration if current_skipped_duration is not None else 600
                # Clear the skipped duration since we've used it
                current_skipped_duration = None

                # Filter titles by minimum duration if specified (min_duration > 0)
                if min_duration <= 0 or duration_seconds >= min_duration:
                    titles.append(TitleInfo(
                        name=f"Episode {title_id}",  # Placeholder, will be parsed later from actual title names
                        duration_minutes=duration_seconds / 60.0,
                        title_id=title_id
                    ))

        # Sort by duration (longest first)
        if titles:
            titles = sorted(titles, key=lambda t: t.duration_minutes, reverse=True)
            logger.info(f"Found {len(titles)} titles from MakeMKV info")

        return titles

    except Exception as e:
        logger.error(f"Error getting title list: {e}")
        return titles


def count_video_files_on_disk(device: str, logger: logging.Logger) -> int:
    """
    Count the number of M2TS video files on the BluRay disk.
    TV shows typically have many small video files, movies have fewer/larger ones.

    Returns count of .m2ts files in BDMV/STREAM directory.
    """
    mount_point = None
    we_mounted = False

    try:
        # Check if device is already mounted
        result = subprocess.run(
            ["findmnt", "-n", "-o", "TARGET", device],
            capture_output=True,
            text=True,
            timeout=5,
        )

        if result.returncode == 0:
            mount_point = result.stdout.strip()
            we_mounted = False
        else:
            mount_point = "/mnt/bluray_count"
            if not mount_device(device, mount_point, logger):
                return -1  # Error
            we_mounted = True

        stream_dir = Path(mount_point) / "BDMV" / "STREAM"

        if not stream_dir.exists():
            return 0

        m2ts_files = list(stream_dir.glob("*.m2ts"))
        count = len(m2ts_files)

        # Log sizes to help distinguish TV vs movie
        if m2ts_files:
            total_size = sum(f.stat().st_size for f in m2ts_files)

        return count

    except Exception as e:
        logger.error(f"Error counting video files: {e}")
        return -1
    finally:
        if we_mounted and mount_point:
            try:
                subprocess.run(
                    ["sudo", "umount", mount_point],
                    capture_output=True,
                    timeout=5,
                )
            except Exception:
                pass


def detect_tv_show_vs_movie(titles: List[TitleInfo], title_name: str, year: int, video_count: int, logger: logging.Logger) -> Tuple[bool, str]:
    """
    Auto-detect if the content is a TV show or movie.

    Detection logic:
    - Video file count > 5 strongly suggests TV show
    - If 3+ titles are over 10 minutes each, it's likely a TV show
    - If exactly 1 dominant title (longest > 80% of total), it's a movie
    - Title name containing "Season" or having year > 2020 with short titles suggests TV

    Returns (is_tv_show, display_name)
    """
    # TV Show criteria 0: Many video files (>5) - strong indicator of TV show
    if video_count > 5:
        logger.info(f"Detected TV show: {video_count} video files found on disk")
        display_name = re.sub(r'\s*[Ss]eason\s*\d+', '', title_name).strip()
        return True, display_name

    # TV Show criteria 1: Title contains "Season"
    if "season" in title_name.lower():
        logger.info(f"Detected TV show: title contains 'season'")
        return True, re.sub(r'\s*[Ss]eason\s*\d+', '', title_name).strip()

    if not titles:
        return False, ""

    # Filter to meaningful content only
    meaningful_titles = [t for t in titles if t.duration_minutes >= 10]

    # Sort by duration (longest first)
    meaningful_titles = sorted(meaningful_titles, key=lambda t: t.duration_minutes, reverse=True)

    total_duration = sum(t.duration_minutes for t in meaningful_titles) if meaningful_titles else 0
    longest_title = meaningful_titles[0] if meaningful_titles else None
    longest_ratio = longest_title.duration_minutes / total_duration if total_duration > 0 else 0

    # TV Show criteria 2: Multiple episodes (3+ titles over 10 min each)
    if len(meaningful_titles) >= 3:
        logger.info(f"Detected TV show: {len(meaningful_titles)} episodes/seasons found")
        first_title = meaningful_titles[0]
        display_name = re.sub(r'\s*[Ee]pisode\s*\d+', '', first_title.name).strip()
        return True, display_name

    # TV Show criteria 3: Recent release (2020+) with short episode-like titles
    if year >= 2020 and len(meaningful_titles) > 1:
        # Check if any titles look like episodes
        has_episode_pattern = any(re.search(r'[Ee]pisode\s*\d+|[Cc]hapter\s*\d+', t.name) for t in meaningful_titles)
        if has_episode_pattern or all(t.duration_minutes < 60 for t in meaningful_titles):
            logger.info(f"Detected TV show: recent release with episode-like content")
            first_title = meaningful_titles[0]
            display_name = re.sub(r'\s*[Ee]pisode\s*\d+', '', first_title.name).strip()
            return True, display_name

    # Movie criteria: 1 dominant title (very long relative to others)
    if longest_ratio > 0.7:
        logger.info(f"Detected movie: single dominant title ({longest_ratio*100:.1f}% of content)")
        return False, longest_title.name if longest_title else title_name

    # Edge case: multiple comparable titles (could be compilation or special)
    logger.warning(f"Multiple similar-length titles detected; using first one")
    return False, meaningful_titles[0].name if meaningful_titles else title_name


# ============================================================================
# MAKEMKV INTEGRATION
# ============================================================================

# ============================================================================
# SETUP
# ============================================================================

def setup_logging(log_level: str, log_file: str) -> logging.Logger:
    """Configure logging to file and stdout."""
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("bluray_ripper")
    logger.setLevel(getattr(logging, log_level.upper()))

    # File handler - writes all levels for debugging
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)

    # Console handler - cleaner output by default
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)  # Only show INFO+ on console

    # Formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


# ============================================================================
# MEDIA DETECTION & OMDB
# ============================================================================

def fetch_omdb_data(title: str, omdb_key: str, logger: logging.Logger, media_type: str = "movie", year: int = 0) -> Optional[MediaInfo]:
    """
    Query OMDb to get movie or TV show metadata.
    OMDb API uses type=series for TV shows, not type=tv.
    Returns MediaInfo or None if not found.

    If year is provided, it will be used to filter results for better accuracy.
    """
    if not omdb_key:
        logger.warning("OMDB_API_KEY not set; skipping metadata lookup")
        return None

    # OMDb uses "series" for TV shows, not "tv"
    omdb_type = media_type
    if media_type == "tv":
        omdb_type = "series"

    url = "https://www.omdbapi.com/"
    params = {"apikey": omdb_key, "t": title, "type": omdb_type}

    # Add year parameter for more accurate matching (e.g., "Knuckles 2023" vs older)
    if year > 0:
        params["y"] = str(year)

    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if data.get("Response") == "False":
            logger.warning(f"OMDb: {data.get('Error', 'Unknown error')}")
            return None

        return MediaInfo(
            title=data.get("Title", title),
            year=int(data.get("Year", 0) or 0),
            imdb_id=data.get("imdbID", ""),
            plot=data.get("Plot", ""),
            rating=data.get("imdbRating", "N/A"),
        )
    except Exception as e:
        logger.error(f"OMDb lookup failed: {e}")
        return None


def fetch_omdb_data_for_tv(title: str, omdb_key: str, logger: logging.Logger, year: int = 0) -> Optional[MediaInfo]:
    """
    Query OMDb to get TV show metadata.
    OMDb API uses type=series for TV shows, not type=tv.
    Returns MediaInfo or None if not found.

    If year is provided, it will be used to filter results for better accuracy.
    """
    return fetch_omdb_data(title, omdb_key, logger, media_type="series", year=year)


# ============================================================================
# SETUP
# ============================================================================

def mount_device(device: str, mount_point: str, logger: logging.Logger) -> bool:
    """Mount optical device if not already mounted."""
    try:
        # Check if already mounted
        result = subprocess.run(
            ["findmnt", "-n", "-o", "TARGET", device],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            return True

        # Mount device
        Path(mount_point).mkdir(parents=True, exist_ok=True)
        result = subprocess.run(
            ["sudo", "mount", device, mount_point],
            capture_output=True,
            text=True,
            timeout=10,
        )
        
        if result.returncode == 0:
            return True
        else:
            return False
    except Exception as e:
        logger.error(f"Mount error: {e}")
        return False


def extract_bluray_title_from_disk(device: str, logger: logging.Logger) -> Optional[str]:
    """
    Extract BluRay disc title from BDMV/META/DL/bdmt_eng.xml metadata file.
    Returns title string or None if not found.
    """
    import xml.etree.ElementTree as ET
    
    mount_point = None
    we_mounted = False  # Track if we did the mounting
    
    try:
        # Check if device is already mounted
        result = subprocess.run(
            ["findmnt", "-n", "-o", "TARGET", device],
            capture_output=True,
            text=True,
            timeout=5,
        )
        
        if result.returncode == 0:
            mount_point = result.stdout.strip()
            we_mounted = False
        else:
            # Device not mounted, mount it ourselves
            mount_point = "/mnt/bluray_detect"
            if not mount_device(device, mount_point, logger):
                return None
            we_mounted = True
        
        # Try to find and parse the metadata XML file
        xml_file = Path(mount_point) / "BDMV" / "META" / "DL" / "bdmt_eng.xml"

        if not xml_file.exists():
            # Try other language variants
            meta_dir = Path(mount_point) / "BDMV" / "META" / "DL"
            if meta_dir.exists():
                xml_files = list(meta_dir.glob("bdmt_*.xml"))
                if xml_files:
                    xml_file = xml_files[0]
                else:
                    return None
            else:
                # List what's actually in the mount point for debugging
                try:
                    bdmv_dir = Path(mount_point) / "BDMV"
                    if not bdmv_dir.exists():
                        return None
                except Exception:
                    return None
                return None
        
        # Parse XML to extract title
        tree = ET.parse(xml_file)
        root = tree.getroot()
        
        # Define namespace for di: prefix
        # The root xmlns="urn:BDA:bdmv;disclib" is the default namespace,
        # and xmlns:di="urn:BDA:bdmv;discinfo" defines the di prefix
        ns = {
            'di': 'urn:BDA:bdmv;discinfo',
        }
        
        # Find the title element using namespace-aware path
        # XPath: .//di:title/di:name (searches anywhere in tree)
        title_elem = root.find(".//di:title/di:name", ns)
        
        if title_elem is not None and title_elem.text:
            return title_elem.text.strip()
        return None
        
    except ET.ParseError as e:
        logger.warning(f"Failed to parse metadata XML: {e}")
        return None
    except Exception as e:
        logger.error(f"Failed to extract title from disk: {e}")
        return None
    finally:
        # Only unmount if we mounted it ourselves
        if we_mounted and mount_point:
            try:
                subprocess.run(
                    ["sudo", "umount", mount_point],
                    capture_output=True,
                    timeout=5,
                )
            except Exception:
                pass


# ============================================================================
# MAKEMKV INTEGRATION
# ============================================================================

def detect_bluray_disk(device: str, logger: logging.Logger) -> bool:
    """Check if a BluRay disk is present in the device via findmnt."""
    try:
        # Use findmnt to check device status
        result = subprocess.run(
            ["findmnt", "-n", "-o", "SOURCE", device],
            capture_output=True,
            text=True,
            timeout=5,
        )
        
        # findmnt returns 0 if device exists and is mounted
        if result.returncode == 0:
            return True

        # Fallback: check if device file exists and is readable
        # This works for unmounted optical media
        device_path = Path(device)
        if device_path.exists() and device_path.is_block_device():
            # Try to read device to confirm it's accessible
            try:
                with open(device, 'rb') as f:
                    f.read(1)
                return True
            except (IOError, OSError):
                pass
            return False

        return False
    except Exception as e:
        logger.error(f"Failed to detect disk: {e}")
        return False


def rip_with_makemkv(
    device: str,
    output_dir: str,
    use_largest_title: bool,
    min_duration: int,
    logger: logging.Logger,
) -> Optional[Path]:
    """
    Rip BluRay to MKV using makemkvcon.
    Shows real-time progress output.
    Returns path to the output MKV, or None if failed.
    Uses title_id=0 to rip all titles.
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    logger.info(f"Starting MakeMKV rip from {device} to {output_dir}")

    try:
        # makemkvcon dev:<device> <title_id> <output_dir>
        cmd = [
            "makemkvcon",
            "--minlength=" + str(min_duration),
            "mkv",
            "dev:" + device,
            "0",
            output_dir,
        ]

        # Run with real-time output streaming
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        # Stream output line by line
        for line in process.stdout:
            line = line.rstrip()
            if line:
                # Log makemkvcon output directly to show progress
                logger.info(f"  {line}")

        process.wait()

        if process.returncode != 0:
            logger.error(f"MakeMKV failed with return code {process.returncode}")
            return None

        # Find the generated MKV files
        mkv_files = list(Path(output_dir).glob("*.mkv"))
        if not mkv_files:
            logger.error("No MKV files found after rip")
            return None

        # If multiple titles, pick the largest
        if use_largest_title and len(mkv_files) > 1:
            mkv_file = max(mkv_files, key=lambda p: p.stat().st_size)
            logger.info(f"Multiple titles found; selecting largest: {mkv_file.name}")
        else:
            # For single title, find the largest (it's usually the main feature)
            if len(mkv_files) > 1:
                mkv_file = max(mkv_files, key=lambda p: p.stat().st_size)
                logger.info(f"Multiple MKVs found; selecting largest: {mkv_file.name}")
            else:
                mkv_file = mkv_files[0]

        logger.info(f"MakeMKV complete: {mkv_file}")
        return mkv_file

    except subprocess.TimeoutExpired:
        logger.error("MakeMKV timed out (>1 hour)")
        return None
    except Exception as e:
        logger.error(f"MakeMKV error: {e}")
        return None


def rip_title_with_makemkv(
    device: str,
    title_id: str,
    output_dir: str,
    min_duration: int,
    logger: logging.Logger,
) -> Optional[Path]:
    """
    Rip a specific title from BluRay to MKV using makemkvcon.
    Unlike rip_with_makemkv(), this rips only the specified title_id.

    Args:
        device: BluRay device path
        title_id: Specific title number to rip (as string)
        output_dir: Output directory for the MKV file
        min_duration: Minimum duration in seconds
        logger: Logger instance

    Returns:
        Path to the output MKV, or None if failed
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    try:
        # makemkvcon dev:<device> <title_id> <output_dir>
        cmd = [
            "makemkvcon",
            "--minlength=" + str(min_duration),
            "mkv",
            "dev:" + device,
            title_id,
            output_dir,
        ]

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        # Stream output line by line (show all MakeMKV progress)
        for line in process.stdout:
            line = line.rstrip()
            if line:
                logger.info(f"  {line}")

        process.wait()

        if process.returncode != 0:
            logger.error(f"MakeMKV failed with return code {process.returncode}")
            return None

        mkv_files = list(Path(output_dir).glob("*.mkv"))
        if not mkv_files:
            logger.error(f"No MKV files found after rip for title {title_id}")
            return None

        mkv_file = max(mkv_files, key=lambda p: p.stat().st_size)
        logger.info(f"MKV complete")
        return mkv_file

    except subprocess.TimeoutExpired:
        logger.error("MakeMKV timed out (>1 hour)")
        return None
    except Exception as e:
        logger.error(f"MakeMKV error: {e}")
        return None


# ============================================================================
# HANDBRAKE INTEGRATION
# ============================================================================

def detect_nvidia_gpu(logger: logging.Logger) -> bool:
    """Check if NVIDIA GPU is available via nvidia-smi."""
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            gpus = result.stdout.strip().split("\n")
            for i, gpu in enumerate(gpus):
                logger.info(f"  Found GPU {i}: {gpu}")
            return True
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return False

def get_available_handbrake_encoders(logger: logging.Logger) -> Dict[str, bool]:
    """
    Check which video encoders are available in HandBrake by parsing --help output.
    Parses the 'Select video encode' section to find available encoders.
    Returns dict of encoder_name -> available (bool).

    Encoder names used: nvenc_h265, nvenc_h264, x265, x264
    """
    try:
        result = subprocess.run(
            ["HandBrakeCLI", "--help"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        output = result.stdout + result.stderr

        # Parse the encoder list from --help output
        # Look for lines after "Select video encode" or "-e, --encoder"
        available_encoders = set()
        in_encoder_section = False

        for line in output.split('\n'):
            if 'Select video encode' in line or '-e, --encoder' in line:
                in_encoder_section = True
                continue
            if in_encoder_section:
                # End of encoder list (next option or empty line after entries)
                line_stripped = line.strip()
                if line_stripped.startswith('-') or not line_stripped:
                    if line_stripped.startswith('-'):
                        break
                    continue
                # Encoder names are indented (e.g., "   nvenc_h265")
                if line.startswith('   ') and line_stripped:
                    available_encoders.add(line_stripped)

        # Map to our encoder categories (using actual HandBrakeCLI names)
        encoders = {
            'nvenc_h265': 'nvenc_h265' in available_encoders,
            'nvenc_h264': 'nvenc_h264' in available_encoders,
            'x265': 'x265' in available_encoders,
            'x264': 'x264' in available_encoders,
        }

        return encoders
    except Exception as e:
        logger.warning(f"Could not detect available encoders: {e}")
        return {'x265': True, 'x264': True}  # Fallback to CPU encoders


def encode_with_handbrake(
    input_mkv: Path,
    output_file: Path,
    handbrake_config: Dict[str, Any],
    logger: logging.Logger,
) -> bool:
    """
    Encode MKV to H.265/AAC using HandBrake CLI.
    Leverages NVIDIA GPU (NVENC) if use_gpu=True and GPU is detected.
    Shows real-time encoding progress.
    Does NOT clean up temp files until encoding succeeds and output is verified.
    Returns True if successful.
    """
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Starting HandBrake encode: {input_mkv.name}")
    
    try:
        # Detect GPU if requested
        use_gpu = handbrake_config.get("use_gpu", False)
        gpu_available = False
        encoders = get_available_handbrake_encoders(logger)
        
        if use_gpu:
            logger.info("Checking for NVIDIA GPU...")
            gpu_available = detect_nvidia_gpu(logger)
            if not gpu_available:
                logger.warning("✗ GPU not found; falling back to CPU")
        
        # Build HandBrake command with opinionated defaults
        cmd = [
            "HandBrakeCLI",
            "-i", str(input_mkv),
            "-o", str(output_file),
            "-Z", handbrake_config["preset"],
            "-f", handbrake_config["format"],
        ]

        selected_encoder = None
        if encoders.get('nvenc_h265'):
            selected_encoder = "nvenc_h265"
            logger.info("Using NVENC H.265")
        elif encoders.get('nvenc_h264'):
            selected_encoder = "nvenc_h264"
            logger.info("Using NVENC H.264")
        else:
            selected_encoder = "x265" if encoders.get('x265') else "x264"

        cmd.extend(["-e", selected_encoder])

        if 'nvenc' in selected_encoder:
            nvidia_quality = max(0, min(51, handbrake_config["quality"] + 3))
            cmd.extend(["-q", str(nvidia_quality)])
        else:
            cmd.extend(["-q", str(handbrake_config["quality"])])

        cmd.extend([
            "--all-audio",
            "-E", handbrake_config["audio_codec"],
            "-B", handbrake_config["audio_bitrate"],
        ])

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        # Stream progress - show all HandBrake output (can take a while)
        for line in iter(process.stdout.readline, ''):
            if not line:
                break
            line = line.rstrip()
            logger.info(f"  {line}")

        process.wait()

        if process.returncode != 0:
            logger.error(f"HandBrake failed with return code {process.returncode}")
            return False

        # Verify output file exists
        try:
            stat_output = subprocess.run(
                ["stat", "--format=%s", str(output_file)],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if stat_output.returncode == 0 and stat_output.stdout.strip():
                logger.info(f"Encoding complete")
                return True
        except Exception:
            pass

        logger.error(f"Output file not found or inaccessible: {output_file}")
        return False

    except subprocess.TimeoutExpired:
        logger.error("HandBrake timed out (>4 hours)")
        return False
    except Exception as e:
        logger.error(f"HandBrake error: {e}")
        return False


# ============================================================================
# ORCHESTRATION
# ============================================================================

def prompt_for_title() -> str:
    """Prompt user for movie title."""
    title = input("\nEnter movie title (for metadata lookup): ").strip()
    return title or "Unknown"


def main():
    # Load configuration FIRST so we can use config file values as argparse defaults
    # First, check if --config was passed on command line (need to parse just this arg)
    config_path = None
    for i, arg in enumerate(sys.argv):
        if arg == "--config" and i + 1 < len(sys.argv):
            config_path = sys.argv[i + 1]
            break
        elif arg.startswith("--config="):
            config_path = arg.split("=", 1)[1]
            break

    config = load_config(config_path)

    parser = argparse.ArgumentParser(
        description="BluRay Ripper - MakeMKV + HandBrake pipeline with Plex organization",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES:
  # Rip from default device (/dev/sr0) to default Plex location
  python rip.py

  # Rip from specific device and output location
  python rip.py --device /dev/dvd --output ~/MyMovies

  # Dry-run to test device detection and metadata
  python rip.py --dry-run

  # Override detected type (movie or tv)
  python rip.py --type tv

        """,
    )

    parser.add_argument(
        "--config",
        help="Path to config YAML file (default: config/defaults.yaml or defaults.yaml)",
    )
    parser.add_argument(
        "--device",
        default=config["device"],
        help=f"BluRay device path (default: {config['device']})",
    )
    parser.add_argument(
        "--output",
        default=config["output_root"],
        dest="output_root",
        help=f"Output root directory (default: {config['output_root']})",
    )
    parser.add_argument(
        "--scratch",
        default=config["scratch_dir"],
        dest="scratch_dir",
        help=f"Scratch directory for MKV temp files (default: {config['scratch_dir']})",
    )
    parser.add_argument(
        "--preset",
        default=config["handbrake"]["preset"],
        help=f"HandBrake preset (default: {config['handbrake']['preset']})",
    )
    parser.add_argument(
        "--log-level",
        default=config["log_level"],
        help="Log level (DEBUG, INFO, WARNING, ERROR)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Check device, mock the pipeline without encoding",
    )
    parser.add_argument(
        "--no-gpu",
        action="store_true",
        help="Disable GPU acceleration (force CPU encoding)",
    )
    parser.add_argument(
        "--type",
        choices=["movie", "tv"],
        dest="media_type",
        help="Override media type detection (movie or tv)",
    )

    args = parser.parse_args()

    # Ensure scratch_dir is a Path object for consistent handling
    if not isinstance(args.scratch_dir, Path):
        args.scratch_dir = Path(args.scratch_dir)

    # Apply CLI overrides to loaded config
    if args.no_gpu:
        config["handbrake"]["use_gpu"] = False

    # Setup
    logger = setup_logging(args.log_level, config["log_file"])

    # Check GPU capability
    if config["handbrake"].get("use_gpu"):
        if detect_nvidia_gpu(logger):
            logger.info("NVENC available")
        else:
            logger.warning("GPU not found; using CPU")

    # Step 1: Detect disk
    logger.info("[1/5] Detecting disk...")
    if not detect_bluray_disk(args.device, logger):
        logger.error(f"No disk detected at {args.device}")
        sys.exit(1)
    logger.info("✓ BluRay disk detected")
    
    # Step 2: Get metadata
    logger.info("\n[2/5] Fetching metadata...")

    # Try to extract title directly from disk first
    disk_title = extract_bluray_title_from_disk(args.device, logger)

    if disk_title:
        title = disk_title
    else:
        # Fallback: prompt user
        title = prompt_for_title()

    video_count = count_video_files_on_disk(args.device, logger)

    # Extract year from title if present (e.g., "Knuckles 2023" -> year=2023)
    year_match = re.search(r'\b(19|20)\d{2}\b', title)
    title_year = int(year_match.group(0)) if year_match else 0

    # Detect TV vs Movie based on video file count
    is_tv_show_by_count = (video_count > 5)

    if is_tv_show_by_count:
        logger.info(f"Detected TV show: {video_count} video files found")
        # Query OMDb for TV series first, then fall back to movie
        media_info = fetch_omdb_data_for_tv(title, config["omdb_api_key"], logger, year=title_year)
        if not media_info:
            media_info = fetch_omdb_data(title, config["omdb_api_key"], logger, year=title_year)
    else:
        # Query OMDb for movie first, then fall back to TV
        media_info = fetch_omdb_data(title, config["omdb_api_key"], logger, year=title_year)
        if not media_info:
            media_info = fetch_omdb_data_for_tv(title, config["omdb_api_key"], logger, year=title_year)

    if media_info:
        # If we extracted a year and OMDb returned something different, trust our extraction
        # This prevents wrong matches like "Knuckles (2023)" when it's actually 2024
        if title_year > 0 and title_year != media_info.year:
            logger.warning(f"OMDb returned {media_info.year} but title suggests {title_year}; using extracted year")

        # Always prefer the disk-extracted year over OMDb's year for folder naming
        if title_year > 0:
            media_info.year = title_year

        title = media_info.title  # Use corrected name from OMDb

        # Print matched metadata for confirmation
        logger.info(f"Matched: {media_info.title} ({media_info.year}) - Type: {media_info.imdb_id}")
    else:
        logger.warning("Metadata lookup failed; using title as-is")
        media_info = MediaInfo(title=title, year=0, imdb_id="", plot="", rating="")

    # Dry-run: stop after metadata fetch
    if args.dry_run:
        logger.info("\n(Dry-run mode; stopping here)")
        # Show both potential outputs based on type detection
        movie_output = Path(args.output_root) / media_info.plex_folder_name()
        tv_root = config.get("output_root_tv_shows", os.path.expanduser(config["output_root"]))
        tv_title_name = re.sub(r'\s*[Ss]eason\s*\d+', '', title).strip() if video_count > 10 else media_info.title
        tv_output = Path(tv_root) / f"{tv_title_name}.tv"
        logger.info(f"Movie output: {movie_output}")
        logger.info(f"TV show output (based on {video_count} video files): {tv_output}")
        # Show detection info
        logger.info(f"Title from disk: {title}")
        logger.info(f"Detected year: {media_info.year}")
        if video_count >= 0:
            logger.info(f"Video files on disk: {video_count}")
        sys.exit(0)
    
    # Step 3: Get title list first to detect content type
    logger.info("Scanning titles...")
    titles = get_makemkv_title_list(args.device, config["makemkv"]["min_duration_seconds"], logger)

    if not titles:
        logger.error("No titles found on disc")
        sys.exit(1)
    logger.info(f"Found {len(titles)} title(s)")

    # Use user override or auto-detect
    if args.media_type:
        is_tv_show = (args.media_type == "tv")
        logger.info(f"--type={args.media_type} override")
    else:
        is_tv_show, tv_title_name = detect_tv_show_vs_movie(titles, title, media_info.year if media_info else 0, video_count, logger)

    # Step 4: Rip based on content type
    logger.info("Ripping content...")

    if is_tv_show:
        # Determine output path for TV shows (Plex structure)
        tv_root = config.get("output_root_tv_shows", os.path.expanduser(config["output_root"]))
        year_str = f" ({media_info.year})" if media_info and media_info.year else ""
        season_folder_format = "Season {:02d}"

        logger.info(f"[TV] {len(titles)} episodes")

        for i, title_info in enumerate(titles):
            episode_num = i + 1
            title_id = title_info.title_id

            # Parse season/episode from title name (e.g., "S01E01", "Episode 01")
            season_match = re.search(r'[Ss](\d+)[Ee](\d+)', title_info.name)
            if season_match:
                season_num = int(season_match.group(1))
                ep_num = int(season_match.group(2))
            else:
                # Fall back to sequential numbering
                season_num = 1
                ep_num = episode_num

            # Extract clean episode name
            clean_name = re.sub(r'[Ss]\d+[Ee]\d+\s*', '', title_info.name).strip()
            if not clean_name:
                clean_name = f"Episode {ep_num:02d}"

            logger.info(f"Episode S{season_num:02d}E{ep_num:02d}: {title_info.name} (ID:{title_id})")

            # Create season folder path
            season_folder = Path(tv_root) / f"{tv_title_name}{year_str}" / season_folder_format.format(season_num)
            season_folder.mkdir(parents=True, exist_ok=True)

            # Output filename (Plex episode naming: ShowName-S##E##.mkv)
            output_filename = f"{tv_title_name.replace(' ', '')}S{season_num:02d}E{ep_num:02d}.mkv"
            output_path = season_folder / output_filename

            # Create temp dir for this episode's MKV (cleanup any leftover files first)
            temp_mkv_dir = Path(args.scratch_dir) / f"episode_{episode_num}"
            if temp_mkv_dir.exists():
                import shutil
                shutil.rmtree(temp_mkv_dir)
            temp_mkv_dir.mkdir(parents=True, exist_ok=True)

            # Step 3b: Rip individual title with MakeMKV
            mkv_path = rip_title_with_makemkv(
                args.device,
                str(title_id),
                str(temp_mkv_dir),
                config["makemkv"]["min_duration_seconds"],
                logger,
            )
            if not mkv_path:
                logger.error(f"MakeMKV rip failed for episode {title_id}")
                sys.exit(1)
            logger.info(f"✓ MKV ready: {mkv_path}")

            # Step 4b: Encode with HandBrake
            if not encode_with_handbrake(mkv_path, output_path, config["handbrake"], logger):
                logger.error("HandBrake encoding failed")
                sys.exit(1)
            logger.info(f"✓ Encoding complete: {output_path}")

            # Cleanup temp MKV for this episode
            try:
                mkv_path.unlink()
            except Exception:
                pass

        logger.info("Done")
    else:
        # Movie handling - rip all titles with MakeMKV
        mkv_path = rip_with_makemkv(
            args.device,
            args.scratch_dir,
            config["makemkv"]["use_largest_title"],
            config["makemkv"]["min_duration_seconds"],
            logger,
        )
        if not mkv_path:
            logger.error("MakeMKV rip failed")
            sys.exit(1)

        folder_name = media_info.plex_folder_name() if media_info else "Unknown Title"
        output_path = Path(args.output_root) / folder_name / f"{folder_name}.mkv"

        # Step 5: Encode with HandBrake
        if not encode_with_handbrake(mkv_path, output_path, config["handbrake"], logger):
            logger.error("HandBrake encoding failed")
            sys.exit(1)

        # Step 6: Cleanup
        try:
            mkv_path.unlink()
        except Exception:
            pass

    logger.info("=" * 40)
    logger.info("Pipeline complete!")
    if is_tv_show:
        logger.info(f"TV show episodes saved to: {tv_root}/{tv_title_name}{year_str}/")
    else:
        logger.info(f"Movie saved to: {output_path}")
        logger.info(f"Plex folder: {output_path.parent}")


if __name__ == "__main__":
    main()
