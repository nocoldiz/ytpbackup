#!/usr/bin/env python3
"""
YouTube Link Extractor & Downloader
====================================
Scans all offline-saved HTML pages from the forum scraper to find
YouTube links, then downloads them via yt-dlp.

Videos are saved into the same section folder structure as the pages.

Auto-resumes: already-downloaded videos are skipped on re-run.

Requirements:
    pip install yt-dlp beautifulsoup4 lxml
    (yt-dlp also needs ffmpeg for merging formats)

Usage:
    python yt_downloader.py                          # Scan & download all
    python yt_downloader.py --scan-only              # Just list found links
    python yt_downloader.py --sections 0,3,7         # Only specific sections
    python yt_downloader.py --format bestaudio       # Audio only
    python yt_downloader.py --max-per-section 10     # Limit downloads per section
    python yt_downloader.py --site-dir ./site_mirror # Custom scraper output dir
"""

import os
import re
import sys
import json
import time
import glob
import logging
import argparse
import subprocess
from pathlib import Path
from collections import OrderedDict
from urllib.parse import urlparse, parse_qs, unquote

from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("yt_dl")

# ─── Section names (must match scraper folder names) ─────────────────────────

SECTIONS = [
    "Bacheca messaggi",
    "Eventi",
    "Restyling",
    "Risorse",
    "Old sources",
    "Biografie YTP",
    "Ganons pub",
    "YTP fai da te",
    "Serve aiuto",
    "Il significato della cacca",
    "Tutorial per il pooping",
    "Poop in progress",
    "YTP da internet",
    "YTP nostrane",
    "YTPMV dimportazione",
    "Collab poopeschi",
    "Club sportivo della foca grassa",
    "Internet memes video",
    "Altri video",
    "Off topic",
    "Videogames",
    "Cinema",
    "Sport",
    "Musica",
    "Arte e grafica",
    "Flood fun",
    "THE PIT",
]

DEFAULT_SITE_DIR = "./site_mirror"
DEFAULT_VIDEO_DIR = "./videos"
DEFAULT_FORMAT = "bestvideo[height<=720]+bestaudio/best[height<=720]/best"

# ─── YouTube URL patterns ────────────────────────────────────────────────────

# Matches all common YouTube URL formats
YT_PATTERNS = [
    # Standard watch URLs
    re.compile(r'https?://(?:www\.)?youtube\.com/watch\?[^\s"\'<>]*v=[\w-]{11}[^\s"\'<>]*', re.I),
    # Short URLs
    re.compile(r'https?://youtu\.be/([\w-]{11})[^\s"\'<>]*', re.I),
    # Embed URLs
    re.compile(r'https?://(?:www\.)?youtube\.com/embed/([\w-]{11})[^\s"\'<>]*', re.I),
    # Shorts
    re.compile(r'https?://(?:www\.)?youtube\.com/shorts/([\w-]{11})[^\s"\'<>]*', re.I),
    # Nocookie embed
    re.compile(r'https?://(?:www\.)?youtube-nocookie\.com/embed/([\w-]{11})[^\s"\'<>]*', re.I),
    # v/ format
    re.compile(r'https?://(?:www\.)?youtube\.com/v/([\w-]{11})[^\s"\'<>]*', re.I),
]

# Extract video ID from any YouTube URL
YT_ID_RE = re.compile(
    r'(?:youtube\.com/(?:watch\?.*?v=|embed/|v/|shorts/)|youtu\.be/|youtube-nocookie\.com/embed/)'
    r'([\w-]{11})',
    re.I,
)


def extract_video_id(url):
    """Extract the 11-character YouTube video ID from a URL."""
    m = YT_ID_RE.search(url)
    return m.group(1) if m else None


def canonical_yt_url(video_id):
    """Return a clean canonical YouTube URL."""
    return f"https://www.youtube.com/watch?v={video_id}"


def safe_filename(name, max_len=80):
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name[:max_len] if len(name) > max_len else name


# ─── Scanner ─────────────────────────────────────────────────────────────────

class YouTubeScanner:
    """Scan saved HTML files for YouTube links."""

    def __init__(self, site_dir):
        self.site_dir = site_dir

    def scan_file(self, filepath):
        """Extract all unique YouTube video IDs from an HTML file."""
        try:
            with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                content = f.read()
        except Exception:
            return set()

        video_ids = set()

        # Method 1: regex on raw HTML (catches everything including JS strings)
        for pattern in YT_PATTERNS:
            for m in pattern.finditer(content):
                url = m.group(0)
                vid = extract_video_id(url)
                if vid:
                    video_ids.add(vid)

        # Method 2: BeautifulSoup for structured extraction
        try:
            soup = BeautifulSoup(content, "lxml")

            # <a href="youtube...">
            for a in soup.find_all("a", href=True):
                vid = extract_video_id(a["href"])
                if vid:
                    video_ids.add(vid)

            # <iframe src="youtube.com/embed/...">
            for iframe in soup.find_all("iframe", src=True):
                vid = extract_video_id(iframe["src"])
                if vid:
                    video_ids.add(vid)

            # <embed>, <object> with YouTube URLs
            for tag in soup.find_all(["embed", "object", "source"]):
                for attr in ("src", "data", "value"):
                    val = tag.get(attr, "")
                    vid = extract_video_id(val)
                    if vid:
                        video_ids.add(vid)

            # <param name="movie" value="youtube...">
            for param in soup.find_all("param"):
                val = param.get("value", "")
                vid = extract_video_id(val)
                if vid:
                    video_ids.add(vid)

        except Exception:
            pass

        return video_ids

    def scan_section(self, section_name):
        """Scan all HTML files in a section folder. Returns {video_id: [source_files]}."""
        section_dir = os.path.join(self.site_dir, safe_filename(section_name))
        if not os.path.isdir(section_dir):
            return {}

        results = {}  # video_id -> list of source HTML files

        # Find all HTML files recursively
        for root, dirs, files in os.walk(section_dir):
            for fname in files:
                if not fname.endswith((".html", ".htm")):
                    continue
                fpath = os.path.join(root, fname)
                ids = self.scan_file(fpath)
                for vid in ids:
                    if vid not in results:
                        results[vid] = []
                    results[vid].append(os.path.relpath(fpath, self.site_dir))

        return results

    def scan_all(self, section_filter=None):
        """Scan all sections. Returns {section_name: {video_id: [files]}}."""
        all_results = OrderedDict()
        sections = section_filter if section_filter else SECTIONS

        for sec in sections:
            results = self.scan_section(sec)
            all_results[sec] = results

        return all_results


# ─── Downloader ──────────────────────────────────────────────────────────────

class YouTubeDownloader:
    """Download YouTube videos using yt-dlp."""

    def __init__(self, video_dir, yt_format, rate_limit=None):
        self.video_dir = video_dir
        self.yt_format = yt_format
        self.rate_limit = rate_limit

        self.state = {"downloaded": [], "failed": [], "unavailable": []}
        self.state_file = os.path.join(video_dir, ".yt_state.json")

    def load_state(self):
        if os.path.exists(self.state_file):
            with open(self.state_file) as f:
                self.state = json.load(f)
                # Ensure all keys exist
                self.state.setdefault("downloaded", [])
                self.state.setdefault("failed", [])
                self.state.setdefault("unavailable", [])

    def save_state(self):
        os.makedirs(self.video_dir, exist_ok=True)
        with open(self.state_file, "w") as f:
            json.dump(self.state, f, indent=2)

    def is_done(self, video_id):
        return (
            video_id in self.state["downloaded"]
            or video_id in self.state["unavailable"]
        )

    def download(self, video_id, output_dir):
        """
        Download a single video. Returns:
          'ok'          - downloaded successfully
          'exists'      - already downloaded
          'unavailable' - video removed/private/blocked
          'error'       - other failure
        """
        if self.is_done(video_id):
            return "exists"

        url = canonical_yt_url(video_id)
        os.makedirs(output_dir, exist_ok=True)

        # Output template: video_id - title.ext
        outtmpl = os.path.join(output_dir, f"%(id)s - %(title).80s.%(ext)s")

        cmd = [
            "yt-dlp",
            "--no-playlist",
            "--no-overwrites",
            "--write-thumbnail",
            "--convert-thumbnails", "jpg",
            "--embed-thumbnail",
            "--add-metadata",
            "--format", self.yt_format,
            "--output", outtmpl,
            "--retries", "3",
            "--socket-timeout", "30",
            "--no-warnings",
        ]

        if self.rate_limit:
            cmd.extend(["--limit-rate", self.rate_limit])

        cmd.append(url)

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300,  # 5 min max per video
            )

            stdout = result.stdout
            stderr = result.stderr

            if result.returncode == 0:
                # Check if it actually downloaded or was already present
                if "has already been downloaded" in stdout:
                    self.state["downloaded"].append(video_id)
                    return "exists"
                self.state["downloaded"].append(video_id)
                return "ok"
            else:
                # Check for known unavailability errors
                unavailable_msgs = [
                    "Video unavailable",
                    "Private video",
                    "This video has been removed",
                    "content is not available",
                    "copyright claim",
                    "account associated with this video has been terminated",
                    "violates YouTube's Terms of Service",
                    "been removed by the uploader",
                    "confirm your age",
                    "Join this channel to get access",
                    "members-only content",
                    "is not available in your country",
                    "video is no longer available",
                ]
                combined = stdout + stderr
                for msg in unavailable_msgs:
                    if msg.lower() in combined.lower():
                        self.state["unavailable"].append(video_id)
                        return "unavailable"

                self.state["failed"].append(video_id)
                return "error"

        except subprocess.TimeoutExpired:
            self.state["failed"].append(video_id)
            return "error"
        except Exception as e:
            log.warning(f"      Exception: {e}")
            self.state["failed"].append(video_id)
            return "error"


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(
        description="Extract YouTube links from scraped forum pages and download them."
    )
    p.add_argument("--site-dir", default=DEFAULT_SITE_DIR,
                   help="Path to the scraped site (default: ./site_mirror)")
    p.add_argument("--video-dir", default=DEFAULT_VIDEO_DIR,
                   help="Where to save videos (default: ./videos)")
    p.add_argument("--format", default=DEFAULT_FORMAT,
                   help="yt-dlp format string (default: 720p best)")
    p.add_argument("--rate-limit", default=None,
                   help="Download rate limit (e.g. 1M, 500K)")
    p.add_argument("--sections", default=None,
                   help="Comma-separated section indices (e.g. 0,1,5)")
    p.add_argument("--max-per-section", type=int, default=None,
                   help="Max videos to download per section")
    p.add_argument("--scan-only", action="store_true",
                   help="Only scan and report links, don't download")
    p.add_argument("--retry-failed", action="store_true",
                   help="Retry previously failed downloads")
    p.add_argument("--list", action="store_true",
                   help="List sections and exit")

    args = p.parse_args()

    if args.list:
        print("\nForum Sections:")
        print(f"{'Idx':<4} {'Name'}")
        print("─" * 50)
        for i, name in enumerate(SECTIONS):
            print(f"{i:<4} {name}")
        sys.exit(0)

    if not os.path.isdir(args.site_dir):
        print(f"Error: site directory not found: {args.site_dir}")
        print("Run the forum scraper first, or use --site-dir to point to it.")
        sys.exit(1)

    # Determine sections to process
    if args.sections is not None:
        indices = [int(x.strip()) for x in args.sections.split(",")]
        section_filter = [SECTIONS[i] for i in indices]
    else:
        section_filter = None

    # ── Phase 1: Scan ──
    print()
    print("=" * 70)
    print("  YouTube Link Extractor & Downloader")
    print(f"  Site dir:   {os.path.abspath(args.site_dir)}")
    print(f"  Video dir:  {os.path.abspath(args.video_dir)}")
    print(f"  Format:     {args.format}")
    print(f"  Scan only:  {'yes' if args.scan_only else 'no'}")
    print("=" * 70)
    print()

    scanner = YouTubeScanner(args.site_dir)
    log.info("🔍 Phase 1: Scanning HTML files for YouTube links...")
    print()

    all_results = scanner.scan_all(section_filter)

    # Display scan results
    grand_total = 0
    grand_unique = set()

    print(f"  {'Section':<40} {'Videos':>7}  {'Unique':>7}")
    print(f"  {'─'*40} {'─'*7}  {'─'*7}")

    for sec_name, videos in all_results.items():
        count = len(videos)
        grand_total += count
        grand_unique.update(videos.keys())

        if count > 0:
            print(f"  📁 {sec_name:<38} {count:>7}")
        else:
            print(f"     {sec_name:<38} {count:>7}")

    print(f"  {'─'*40} {'─'*7}  {'─'*7}")
    print(f"  {'TOTAL (per-section)':<40} {grand_total:>7}")
    print(f"  {'UNIQUE (deduplicated)':<40} {len(grand_unique):>7}")
    print()

    if args.scan_only:
        # Detailed output
        for sec_name, videos in all_results.items():
            if not videos:
                continue
            print(f"\n{'─' * 70}")
            print(f"  {sec_name} ({len(videos)} videos)")
            print(f"{'─' * 70}")
            for vid, sources in sorted(videos.items()):
                url = canonical_yt_url(vid)
                print(f"  {url}")
                for src in sources[:3]:  # Show up to 3 source files
                    print(f"    └─ {src}")
                if len(sources) > 3:
                    print(f"    └─ ... and {len(sources)-3} more")
        print()
        sys.exit(0)

    # ── Phase 2: Download ──
    log.info("📥 Phase 2: Downloading videos...")
    print()

    downloader = YouTubeDownloader(args.video_dir, args.format, args.rate_limit)
    downloader.load_state()

    if args.retry_failed:
        failed_before = downloader.state.get("failed", [])
        if failed_before:
            log.info(f"  Retrying {len(failed_before)} previously failed videos...")
            downloader.state["failed"] = []
            downloader.save_state()

    total_ok = 0
    total_skip = 0
    total_unavail = 0
    total_err = 0

    for sec_name, videos in all_results.items():
        if not videos:
            continue

        section_video_dir = os.path.join(args.video_dir, safe_filename(sec_name))
        video_ids = list(videos.keys())

        # Count already done
        already_done = sum(1 for v in video_ids if downloader.is_done(v))
        to_do = len(video_ids) - already_done

        if args.max_per_section:
            video_ids = video_ids[:args.max_per_section]

        print(f"{'─' * 70}")
        print(f"  📁 {sec_name}")
        print(f"     {len(video_ids)} videos ({already_done} already done, {to_do} remaining)")
        print(f"{'─' * 70}")

        sec_ok = 0
        sec_skip = 0
        sec_unavail = 0
        sec_err = 0

        for i, vid in enumerate(video_ids, 1):
            if downloader.is_done(vid) and not args.retry_failed:
                sec_skip += 1
                continue

            pct = i / len(video_ids) * 100
            url = canonical_yt_url(vid)
            log.info(f"    [{i}/{len(video_ids)}] ({pct:.0f}%) {url}")

            result = downloader.download(vid, section_video_dir)

            if result == "ok":
                log.info(f"      ✓ Downloaded")
                sec_ok += 1
            elif result == "exists":
                sec_skip += 1
            elif result == "unavailable":
                log.info(f"      ⊘ Unavailable (removed/private)")
                sec_unavail += 1
            else:
                log.info(f"      ✗ Failed")
                sec_err += 1

            # Save state after each video
            downloader.save_state()

            # Small delay between downloads to be nice
            if result == "ok":
                time.sleep(1)

        total_ok += sec_ok
        total_skip += sec_skip
        total_unavail += sec_unavail
        total_err += sec_err

        done_now = sec_ok + sec_skip + sec_unavail
        sec_total = len(video_ids)
        pct = (done_now / sec_total * 100) if sec_total > 0 else 100
        log.info(
            f"  ✅ {sec_name}: {sec_ok} new, {sec_skip} skipped, "
            f"{sec_unavail} unavailable, {sec_err} failed"
        )

    downloader.save_state()

    # ── Summary ──
    print()
    print("=" * 70)
    print("  DOWNLOAD SUMMARY")
    print("=" * 70)
    print(f"  Downloaded:   {total_ok}")
    print(f"  Skipped:      {total_skip} (already done)")
    print(f"  Unavailable:  {total_unavail} (removed/private)")
    print(f"  Failed:       {total_err} (use --retry-failed)")
    print(f"  Videos dir:   {os.path.abspath(args.video_dir)}")
    print()

    # Per-section breakdown
    print(f"  {'Section':<40} {'DL':>4} {'Skip':>5} {'N/A':>5} {'Err':>4}")
    print(f"  {'─'*40} {'─'*4} {'─'*5} {'─'*5} {'─'*4}")

    for sec_name, videos in all_results.items():
        if not videos:
            continue
        sec_dl = sum(1 for v in videos if v in downloader.state["downloaded"])
        sec_na = sum(1 for v in videos if v in downloader.state["unavailable"])
        sec_fa = sum(1 for v in videos if v in downloader.state["failed"])
        sec_total = len(videos)
        sec_skip = sec_total - sec_dl - sec_na - sec_fa

        bar_len = 12
        done = sec_dl + sec_na
        filled = int(bar_len * done / sec_total) if sec_total > 0 else 0
        bar = "█" * filled + "░" * (bar_len - filled)

        print(f"  {sec_name:<40} {sec_dl:>4} {sec_skip:>5} {sec_na:>5} {sec_fa:>4}  {bar}")

    print()
    print(f"  Run again to resume. Use --retry-failed to retry errors.")
    print("=" * 70)


if __name__ == "__main__":
    main()
