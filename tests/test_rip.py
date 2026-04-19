#!/usr/bin/env python3
"""
Unit tests for rip.py TV show support and auto-detection.
Run with: python tests/test_rip.py
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))


class MockLogger:
    """Simple mock logger for testing that supports debug/info/warning/error calls."""
    def __init__(self):
        self.debug_calls = []
        self.info_calls = []
        self.warning_calls = []
        self.error_calls = []

    def debug(self, msg):
        self.debug_calls.append(msg)

    def info(self, msg):
        self.info_calls.append(msg)

    def warning(self, msg):
        self.warning_calls.append(msg)

    def error(self, msg):
        self.error_calls.append(msg)


def test_titleinfo_creation():
    """Test creating a TitleInfo object."""
    from rip import TitleInfo
    title = TitleInfo(name="Test Movie", duration_minutes=120)
    assert title.name == "Test Movie"
    assert title.duration_minutes == 120

    logger = MockLogger()  # Ensure we have a logger for error handling in tests that need it


def test_titleinfo_zero_duration():
    """Test TitleInfo with zero duration."""
    from rip import TitleInfo
    title = TitleInfo(name="Short", duration_minutes=0)
    assert title.duration_minutes == 0

    logger = MockLogger()  # Ensure we have a logger for error handling in tests that need it


def test_movie_single_dominant_title():
    """Test detection of movie with single dominant title (2+ meaningful titles, 1 dominant)."""
    from rip import TitleInfo, detect_tv_show_vs_movie
    # Movie: 2 meaningful titles where longest is >70% of total
    titles = [
        TitleInfo(name="Main Movie", duration_minutes=100),   # 100m
        TitleInfo(name="Short Clip", duration_minutes=30),     # 30m
    ]
    logger = MockLogger()
    is_tv_show, display_name = detect_tv_show_vs_movie(titles, logger)
    assert not is_tv_show, f"Expected movie but got TV show: {is_tv_show}"
    assert display_name == "Main Movie", f"Expected 'Main Movie' but got '{display_name}'"


def test_tv_show_three_titles():
    """Test detection of TV show with 3+ titles."""
    from rip import TitleInfo, detect_tv_show_vs_movie
    # TV Show: 3 meaningful titles (10+ minutes each)
    # First episode is longest to ensure it's detected as display_name
    titles = [
        TitleInfo(name="Episode 1", duration_minutes=48),
        TitleInfo(name="Episode 2", duration_minutes=45),
        TitleInfo(name="Episode 3", duration_minutes=46),
    ]
    logger = MockLogger()
    is_tv_show, display_name = detect_tv_show_vs_movie(titles, logger)
    assert is_tv_show, f"Expected TV show but got movie: {is_tv_show}"
    assert display_name == "Episode 1", f"Expected 'Episode 1' but got '{display_name}'"


def test_no_titles():
    """Test handling of empty titles list."""
    from rip import TitleInfo, detect_tv_show_vs_movie
    logger = MockLogger()
    is_tv_show, display_name = detect_tv_show_vs_movie([], logger)
    assert not is_tv_show, f"Expected False but got {is_tv_show}"
    assert display_name == "", f"Expected empty string but got '{display_name}'"


def test_edge_case_two_similar_titles():
    """Test edge case with two similar-length titles."""
    from rip import TitleInfo, detect_tv_show_vs_movie
    titles = [
        TitleInfo(name="Title A", duration_minutes=60),
        TitleInfo(name="Title B", duration_minutes=55),
    ]
    logger = MockLogger()
    is_tv_show, display_name = detect_tv_show_vs_movie(titles, logger)
    # Should treat as movie (edge case)
    assert not is_tv_show
    assert display_name == "Title A"


if __name__ == "__main__":
    from sys import exit

    print("Running rip.py unit tests...")
    print("=" * 50)

    tests = [
        ("test_titleinfo_creation", test_titleinfo_creation),
        ("test_titleinfo_zero_duration", test_titleinfo_zero_duration),
        ("test_movie_single_dominant_title", test_movie_single_dominant_title),
        ("test_tv_show_three_titles", test_tv_show_three_titles),
        ("test_no_titles", test_no_titles),
        ("test_edge_case_two_similar_titles", test_edge_case_two_similar_titles),
    ]

    passed = 0
    failed = 0

    for name, func in tests:
        try:
            result = func()
            # Check for debug/info calls that indicate detection result
            if result and hasattr(result, "debug_calls"):
                print(f"    [DETECT] {result.debug_calls}")
            print(f"[PASS] {name}")
            passed += 1
        except AssertionError as e:
            msg = str(e) or "[no message]"
            print(f"[FAIL] {name}: {msg}")
            # Show debug output for debugging if available
            failed += 1
        except Exception as e:
            print(f"[ERROR] {name}: {type(e).__name__}: {e}")
            failed += 1

    print("=" * 50)
    print(f"Results: {passed} passed, {failed} failed")

    exit(0 if failed == 0 else 1)
