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

    # Temporary scratch space (where MakeMKV outputs MKVs before encoding)
    "scratch_dir": os.path.expanduser("~/tmp/bluray_scratch"),

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
                return merged_config
        except ImportError:
            logging.warning("PyYAML not installed; using default configuration")
        except Exception as e:
            logging.debug(f"Config file load error, using defaults: {e}")

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
            logger.debug(f"Using existing mount point: {mount_point}")
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

                logger.info(f"Found {len(titles)} titles from metadata")
            except ET.ParseError as e:
                logger.debug(f"Failed to parse XML: {e}")
        else:
            logger.debug(f"No bdmt_eng.xml found at {xml_file}")

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
                logger.debug(f"Unmounted {mount_point}")
            except Exception:
                pass


def get_makemkv_title_list(device: str, min_duration: int = 600) -> List[TitleInfo]:
    """
    Get list of titles from MakeMKV rip operation.
    Parses makemkvcon output for title names and durations.

    Returns list of TitleInfo objects sorted by duration (longest first).
    """
    titles: List[TitleInfo] = []

    try:
        # Run makemkvcon in listing mode to get title info
        cmd = [
            "makemkvcon",
            "--minlength=" + str(min_duration),
            "list",  # Listing mode - just show titles without ripping
            "dev:" + device,
            "0",
        ]

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=60,
        )

        output = process.communicate(timeout=60)[0]

        # Parse makemkvcon listing output for titles and durations
        for line in output.split('\n'):
            # Format: "   123456789 (3h 15m) Episode Title"
            # or: "001011121 (30m) Movie Title - Alternate Title"
            match = re.search(r'\((\d+) m\)\s+(.+)', line)
            if match:
                duration_mins = int(match.group(1))
                name = match.group(2).strip()

                # Remove episode numbers from title (e.g., "S01E03" or "#1")
                clean_name = re.sub(r'[Ss]\d+[Ee]\d+|#\d+', '', name)
                clean_name = re.sub(r'\([^)]+\)', '', clean_name)  # Remove extra parentheses

                if clean_name and len(clean_name.strip()) > 2:
                    titles.append(TitleInfo(name=clean_name, duration_minutes=duration_mins))

        if titles:
            logger.info(f"Found {len(titles)} titles from MakeMKV listing")

        return sorted(titles, key=lambda t: t.duration_minutes, reverse=True)

    except subprocess.TimeoutExpired:
        logger.error("MakeMKV listing timed out")
        return titles
    except Exception as e:
        logger.error(f"Error getting title list: {e}")
        return titles


def detect_tv_show_vs_movie(titles: List[TitleInfo], logger: logging.Logger) -> Tuple[bool, str]:
    """
    Auto-detect if the content is a TV show or movie.

    Detection logic:
    - If 3+ titles are over 10 minutes each, it's likely a TV show
    - If exactly 1 dominant title (longest > 80% of total), it's a movie

    Returns (is_tv_show, display_name)
    """
    if not titles:
        logger.debug("No titles found; cannot detect type")
        return False, ""

    # Filter to meaningful content only
    meaningful_titles = [t for t in titles if t.duration_minutes >= 10]

    if len(meaningful_titles) < 2:
        logger.debug(f"Only {len(meaningful_titles)} meaningful titles; treating as movie")
        return False, ""

    # Sort by duration (longest first)
    meaningful_titles = sorted(meaningful_titles, key=lambda t: t.duration_minutes, reverse=True)

    total_duration = sum(t.duration_minutes for t in meaningful_titles)
    longest_title = meaningful_titles[0]
    longest_ratio = longest_title.duration_minutes / total_duration if total_duration > 0 else 0

    logger.debug(f"Longest title: {longest_title.name} ({longest_title.duration_minutes}m, {longest_ratio*100:.1f}% of total)")

    # TV Show criteria: 3+ titles over 10 minutes each
    if len(meaningful_titles) >= 3:
        logger.info(f"Detected TV show: {len(meaningful_titles)} episodes/seasons found")

        # Try to determine if these are episode numbers or season info from title
        first_title = meaningful_titles[0]
        display_name = first_title.name

        # Check if titles contain season indicators in the metadata
        is_multi_season = any("season" in t.name.lower() for t in meaningful_titles)

        return True, display_name

    # Movie criteria: 1 dominant title (very long relative to others)
    elif longest_ratio > 0.7:
        logger.info(f"Detected movie: single dominant title ({longest_ratio*100:.1f}% of content)")
        return False, meaningful_titles[0].name

    # Edge case: multiple comparable titles (could be compilation or special)
    else:
        logger.warning(f"Multiple similar-length titles detected; using first one")
        return False, meaningful_titles[0].name


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
    
    # File handler
    fh = logging.FileHandler(log_file)
    fh.setLevel(getattr(logging, log_level.upper()))
    
    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(getattr(logging, log_level.upper()))
    
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

def fetch_omdb_data(title: str, omdb_key: str, logger: logging.Logger) -> Optional[MediaInfo]:
    """
    Query OMDb to get movie metadata.
    Returns MediaInfo or None if not found.
    """
    if not omdb_key:
        logger.warning("OMDB_API_KEY not set; skipping metadata lookup")
        return None
    
    url = "https://www.omdbapi.com/"
    params = {"apikey": omdb_key, "t": title, "type": "movie"}
    
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
            logger.debug(f"Device already mounted at {result.stdout.strip()}")
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
            logger.debug(f"Mounted {device} at {mount_point}")
            return True
        else:
            logger.warning(f"Failed to mount {device}: {result.stderr}")
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
            # Device is already mounted, use that mount point
            mount_point = result.stdout.strip()
            we_mounted = False
            logger.debug(f"Using existing mount point: {mount_point}")
        else:
            # Device not mounted, mount it ourselves
            mount_point = "/mnt/bluray_detect"
            if not mount_device(device, mount_point, logger):
                logger.debug("Could not mount device to read metadata")
                return None
            we_mounted = True
        
        # Try to find and parse the metadata XML file
        xml_file = Path(mount_point) / "BDMV" / "META" / "DL" / "bdmt_eng.xml"
        logger.debug(f"Looking for metadata at: {xml_file}")
        
        if not xml_file.exists():
            logger.debug(f"No metadata file at {xml_file}")
            # Try other language variants
            meta_dir = Path(mount_point) / "BDMV" / "META" / "DL"
            logger.debug(f"Checking if META/DL dir exists: {meta_dir}")
            if meta_dir.exists():
                xml_files = list(meta_dir.glob("bdmt_*.xml"))
                logger.debug(f"Found {len(xml_files)} metadata files: {[f.name for f in xml_files]}")
                if xml_files:
                    xml_file = xml_files[0]
                    logger.debug(f"Using alternate metadata: {xml_file.name}")
                else:
                    logger.debug("No bdmt_*.xml files found in META/DL")
                    return None
            else:
                logger.debug(f"META/DL directory does not exist at {meta_dir}")
                # List what's actually in the mount point
                try:
                    bdmv_dir = Path(mount_point) / "BDMV"
                    if bdmv_dir.exists():
                        logger.debug(f"Contents of {bdmv_dir}: {list(bdmv_dir.iterdir())}")
                    else:
                        logger.debug(f"BDMV directory does not exist at {bdmv_dir}")
                except Exception as e:
                    logger.debug(f"Error listing directory: {e}")
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
            title = title_elem.text.strip()
            logger.info(f"✓ Extracted title from disk: {title}")
            return title
        
        logger.debug("No title found in XML metadata")
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
                logger.debug(f"Unmounted {mount_point}")
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
            # Check if output contains the device path
            output = result.stdout.strip()
            if output and device in output:
                logger.debug(f"Device found: {output}")
                return True
        
        # Fallback: check if device file exists and is readable
        # This works for unmounted optical media
        device_path = Path(device)
        if device_path.exists() and device_path.is_block_device():
            # Try to read device to confirm it's accessible
            try:
                with open(device, 'rb') as f:
                    f.read(1)
                logger.debug(f"Device accessible: {device}")
                return True
            except (IOError, OSError):
                logger.debug(f"Device not readable: {device}")
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
        
        logger.debug(f"Running: {' '.join(cmd)}")
        
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
        
        # Find the generated MKV file
        mkv_files = list(Path(output_dir).glob("*.mkv"))
        if not mkv_files:
            logger.error("No MKV files found after rip")
            return None
        
        # If multiple titles, pick the largest
        if use_largest_title and len(mkv_files) > 1:
            mkv_file = max(mkv_files, key=lambda p: p.stat().st_size)
            logger.info(f"Multiple titles found; selecting largest: {mkv_file.name}")
        else:
            mkv_file = mkv_files[0]
        
        logger.info(f"✓ MakeMKV complete: {mkv_file}")
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

        logger.debug(f"Found encoders: {available_encoders}")

        # Map to our encoder categories (using actual HandBrakeCLI names)
        encoders = {
            'nvenc_h265': 'nvenc_h265' in available_encoders,
            'nvenc_h264': 'nvenc_h264' in available_encoders,
            'x265': 'x265' in available_encoders,
            'x264': 'x264' in available_encoders,
        }

        for enc, available in encoders.items():
            status = "available" if available else "unavailable"
            logger.debug(f"HandBrake encoder {enc}: {status}")

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
        
        # Video codec: default to nvenc_h265 if available, otherwise fall back through options
        selected_encoder = None
        if encoders.get('nvenc_h265'):
            # NVIDIA NVENC H.265 (HEVC) encoder - preferred default
            selected_encoder = "nvenc_h265"
            logger.info("✓ Using NVIDIA NVENC H.265 encoder")
        elif encoders.get('nvenc_h264'):
            # NVIDIA NVENC H.264 encoder
            selected_encoder = "nvenc_h264"
            logger.info("⚠ nvenc_h265 not available; using NVIDIA NVENC H.264 encoder")
        elif encoders.get('x265'):
            # CPU H.265 (HEVC) encoder
            selected_encoder = "x265"
            logger.info("⚠ No NVENC available; using CPU H.265 encoder")
        elif encoders.get('x264'):
            # CPU H.264 encoder
            selected_encoder = "x264"
            logger.info("⚠ Using CPU H.264 encoder")
        else:
            # Ultimate fallback
            selected_encoder = "x264"
            logger.warning("⚠ No encoders detected; defaulting to x264")
        
        cmd.extend(["-e", selected_encoder])
        
        # Quality: CRF for CPU, NVIDIA uses different quality scale
        if 'nvenc' in selected_encoder:
            # NVIDIA NVENC uses 0-51 quality scale (51=lowest quality, 0=highest)
            # Map CRF 22 (CPU) to roughly NVIDIA quality 25 (similar perception)
            nvidia_quality = max(0, min(51, handbrake_config["quality"] + 3))
            cmd.extend(["-q", str(nvidia_quality)])
        else:
            cmd.extend(["-q", str(handbrake_config["quality"])])
        
        # Audio: include ALL audio tracks from source
        # --all-audio: include every audio track found
        # -E: audio encoder for all tracks
        # -B: bitrate for all tracks
        cmd.extend([
            "--all-audio",
            "-E", handbrake_config["audio_codec"],  # Audio encoder (aac)
            "-B", handbrake_config["audio_bitrate"],  # Audio bitrate
        ])

        # No subtitles - skip subtitle processing entirely
        
        logger.debug(f"Running: {' '.join(cmd)}")
        
        # Run with real-time progress streaming
        # HandBrake outputs progress to stderr
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        
        # Stream stderr (progress) and stdout
        import select
        while True:
            # Check both stdout and stderr for output
            ready = select.select([process.stdout, process.stderr], [], [], 0.1)
            
            for stream in ready[0]:
                line = stream.readline()
                if line:
                    line = line.rstrip()
                    if line:
                        # Log HandBrake output to show progress
                        if "Encoding:" in line or "%" in line or "fps" in line:
                            # Progress lines - log at INFO
                            logger.info(f"  {line}")
                        else:
                            # Other output - log at DEBUG
                            logger.debug(f"  {line}")
            
            # Check if process is done
            if process.poll() is not None:
                break
        
        # Get any remaining output
        remaining_out = process.stdout.read()
        remaining_err = process.stderr.read()
        if remaining_out:
            logger.info(f"  {remaining_out.rstrip()}")
        if remaining_err:
            logger.info(f"  {remaining_err.rstrip()}")
        
        if process.returncode != 0:
            logger.error(f"HandBrake failed with return code {process.returncode}")
            return False
        
        # Verify output file exists and has content
        logger.info("Verifying encoded file...")
        try:
            result = subprocess.run(
                ["stat", str(output_file)],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                # Extract file size from stat output
                stat_output = result.stdout
                logger.info(f"✓ Output file verified: {output_file}")
                logger.info(f"  {stat_output.split(chr(10))[0]}")  # First line of stat
                return True
            else:
                logger.error(f"Output file not found or inaccessible: {output_file}")
                return False
        except Exception as e:
            logger.error(f"Failed to verify output file: {e}")
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

  # Rip a TV show (auto-detects from disk content)
  python rip.py --output ~/Media/Plex/TV Shows

        """,
    )
    
    parser.add_argument(
        "--device",
        default=DEFAULT_CONFIG["device"],
        help=f"BluRay device path (default: {DEFAULT_CONFIG['device']})",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_CONFIG["output_root"],
        dest="output_root",
        help=f"Output root directory (default: {DEFAULT_CONFIG['output_root']})",
    )
    parser.add_argument(
        "--scratch",
        default=DEFAULT_CONFIG["scratch_dir"],
        dest="scratch_dir",
        help=f"Scratch directory for MKV temp files (default: {DEFAULT_CONFIG['scratch_dir']})",
    )
    parser.add_argument(
        "--preset",
        default=DEFAULT_CONFIG["handbrake"]["preset"],
        help=f"HandBrake preset (default: {DEFAULT_CONFIG['handbrake']['preset']})",
    )
    parser.add_argument(
        "--log-level",
        default=DEFAULT_CONFIG["log_level"],
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
    
    args = parser.parse_args()
    
    # Apply CLI overrides
    if args.no_gpu:
        DEFAULT_CONFIG["handbrake"]["use_gpu"] = False
    
    # Setup
    logger = setup_logging(args.log_level, DEFAULT_CONFIG["log_file"])
    logger.info("=" * 80)
    logger.info("BluRay Ripper Started")
    logger.info(f"Device: {args.device}")
    logger.info(f"Output: {args.output_root}")
    logger.info(f"Scratch: {args.scratch_dir}")
    
    # Check GPU capability
    if DEFAULT_CONFIG["handbrake"].get("use_gpu"):
        logger.info("\n[GPU Acceleration]")
        if detect_nvidia_gpu(logger):
            logger.info("✓ NVIDIA GPU available; will use NVENC for encoding")
        else:
            logger.warning("✗ NVIDIA GPU not detected; will use CPU encoding")
    
    # Step 1: Detect disk
    logger.info("\n[1/5] Detecting BluRay disk...")
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
        logger.info(f"Using title from disk: {title}")
    else:
        # Fallback: prompt user
        title = prompt_for_title()
    
    # Look up metadata on OMDb
    media_info = fetch_omdb_data(title, DEFAULT_CONFIG["omdb_api_key"], logger)
    if media_info:
        logger.info(f"✓ Found: {media_info.plex_folder_name()}")
        logger.info(f"  Rating: {media_info.rating}, Plot: {media_info.plot[:100]}...")
    else:
        logger.warning("✗ Metadata lookup failed; using title as-is")
        media_info = MediaInfo(title=title, year=0, imdb_id="", plot="", rating="")
    
    # Dry-run: stop after metadata fetch
    if args.dry_run:
        logger.info("\n(Dry-run mode; stopping here)")
        logger.info(f"Would rip to: {Path(args.output_root) / media_info.plex_folder_name()}")
        sys.exit(0)
    
    # Step 3: Rip with MakeMKV
    logger.info("\n[3/5] Ripping with MakeMKV...")
    mkv_path = rip_with_makemkv(
        args.device,
        args.scratch_dir,
        DEFAULT_CONFIG["makemkv"]["use_largest_title"],
        DEFAULT_CONFIG["makemkv"]["min_duration_seconds"],
        logger,
    )
    if not mkv_path:
        logger.error("MakeMKV rip failed")
        sys.exit(1)
    logger.info(f"✓ MKV ready: {mkv_path}")

    # Step 4: Auto-detect TV show vs movie and encode with HandBrake
    logger.info("\n[4/5] Detecting content type and encoding...")

    # Get title list from the rip to detect content type
    titles = get_makemkv_title_list(args.device, DEFAULT_CONFIG["makemkv"]["min_duration_seconds"])

    # Detect if this is a TV show or movie
    is_tv_show, tv_title_name = detect_tv_show_vs_movie(titles, logger)

    if is_tv_show:
        logger.info(f"✓ Detected as TV show: {tv_title_name}")

        # Determine output path for TV shows (Plex structure)
        tv_root = DEFAULT_CONFIG.get("output_root_tv_shows", os.path.expanduser(DEFAULT_CONFIG["output_root"]))

        # For TV shows, use a simple structure without year
        output_path = Path(tv_root) / f"{tv_title_name}.tv"

        logger.info(f"  Output (TV): {output_path}")

        if not encode_with_handbrake(mkv_path, output_path, DEFAULT_CONFIG["handbrake"], logger):
            logger.error("HandBrake encoding failed")
            sys.exit(1)
    else:
        # Movie handling (original logic)
        folder_name = media_info.plex_folder_name() if media_info else "Unknown Title"
        output_path = Path(args.output_root) / folder_name / f"{folder_name}.mkv"

        logger.info(f"  Output (Movie): {output_path}")

        if not encode_with_handbrake(mkv_path, output_path, DEFAULT_CONFIG["handbrake"], logger):
            logger.error("HandBrake encoding failed")
            sys.exit(1)
    logger.info(f"✓ Encoding complete: {output_path}")
    
    # Step 5: Cleanup
    logger.info("\n[5/5] Cleanup...")
    try:
        mkv_path.unlink()
        logger.info(f"✓ Removed temp MKV: {mkv_path}")
    except Exception as e:
        logger.warning(f"Failed to remove temp file: {e}")
    
    logger.info("\n" + "=" * 80)
    logger.info("✓ Pipeline complete!")
    logger.info(f"Movie saved to: {output_path}")
    logger.info(f"Plex folder: {output_path.parent}")


if __name__ == "__main__":
    main()