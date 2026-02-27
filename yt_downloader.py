#!/usr/bin/env python3
"""
YouTube Link Extractor & Downloader
====================================
Scans offline-saved forum pages for YouTube links, downloads them via
yt-dlp, organized by forum section.

Videos are saved into:
    videos/<Section Name>/<video_id> - <title>.mp4

A video_index.json is maintained with full associations:
    {
      "dQw4w9WgXcQ": {
        "url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        "title": "Never Gonna Give You Up",
        "sections": ["YTP da internet", "Off topic"],
        "source_pages": [
          "YTP da internet/12345_Thread Title.html",
          "Off topic/67890_Another Thread/page_1.html"
        ],
        "status": "downloaded",
        "local_file": "videos/YTP da internet/dQw4w9WgXcQ - Never Gonna Give You Up.mp4"
      }
    }

Auto-resumes: already-downloaded videos are skipped on re-run.

Requirements:
    pip install yt-dlp beautifulsoup4 lxml
    (yt-dlp also needs ffmpeg for merging formats)

Usage:
    python yt_downloader.py                          # Scan & download all
    python yt_downloader.py --scan-only              # Just list found links
    python yt_downloader.py --sections 0,3,7         # Only specific sections
    python yt_downloader.py --format bestaudio       # Audio only
    python yt_downloader.py --max-per-section 10     # Limit per section
    python yt_downloader.py --site-dir ./site_mirror # Custom scraper output dir
    python yt_downloader.py --retry-failed           # Retry previous failures
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
from urllib.parse import urlparse, parse_qs

from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("yt_dl")

# ─── Section names (match scraper folder names) ──────────────────────────────

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

YT_PATTERNS = [
    re.compile(r'https?://(?:www\.)?youtube\.com/watch\?[^\s"\'<>]*v=[\w-]{11}[^\s"\'<>]*', re.I),
    re.compile(r'https?://youtu\.be/([\w-]{11})[^\s"\'<>]*', re.I),
    re.compile(r'https?://(?:www\.)?youtube\.com/embed/([\w-]{11})[^\s"\'<>]*', re.I),
    re.compile(r'https?://(?:www\.)?youtube\.com/shorts/([\w-]{11})[^\s"\'<>]*', re.I),
    re.compile(r'https?://(?:www\.)?youtube-nocookie\.com/embed/([\w-]{11})[^\s"\'<>]*', re.I),
    re.compile(r'https?://(?:www\.)?youtube\.com/v/([\w-]{11})[^\s"\'<>]*', re.I),
]

YT_ID_RE = re.compile(
    r'(?:youtube\.com/(?:watch\?.*?v=|embed/|v/|shorts/)|youtu\.be/|youtube-nocookie\.com/embed/)'
    r'([\w-]{11})',
    re.I,
)


def extract_video_id(url):
    m = YT_ID_RE.search(url)
    return m.group(1) if m else None


def canonical_yt_url(video_id):
    return f"https://www.youtube.com/watch?v={video_id}"


def safe_filename(name, max_len=80):
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name[:max_len] if len(name) > max_len else name


# ─── Video Index ─────────────────────────────────────────────────────────────

class VideoIndex:
    """
    Maintains video_index.json with full video-to-page associations.

    Structure:
    {
      "<video_id>": {
        "url": "https://www.youtube.com/watch?v=...",
        "title": "Video Title" | null,
        "sections": ["Section A", "Section B"],
        "source_pages": ["Section A/12345_Thread.html", ...],
        "status": "pending" | "downloaded" | "unavailable" | "failed",
        "local_file": "videos/Section A/ID - Title.mp4" | null
      }
    }
    """

    def __init__(self, video_dir):
        self.video_dir = video_dir
        self.filepath = os.path.join(video_dir, "video_index.json")
        self.data = {}

    def load(self):
        if os.path.exists(self.filepath):
            with open(self.filepath) as f:
                self.data = json.load(f)

    def save(self):
        os.makedirs(self.video_dir, exist_ok=True)
        with open(self.filepath, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)

    def add_video(self, video_id, section, source_page):
        """Register a video found in a source page."""
        if video_id not in self.data:
            self.data[video_id] = {
                "url": canonical_yt_url(video_id),
                "title": None,
                "sections": [],
                "source_pages": [],
                "status": "pending",
                "local_file": None,
            }

        entry = self.data[video_id]

        if section not in entry["sections"]:
            entry["sections"].append(section)

        if source_page not in entry["source_pages"]:
            entry["source_pages"].append(source_page)

    def get_status(self, video_id):
        if video_id in self.data:
            return self.data[video_id]["status"]
        return "pending"

    def is_done(self, video_id):
        s = self.get_status(video_id)
        return s in ("downloaded", "unavailable")

    def set_downloaded(self, video_id, local_file, title=None):
        if video_id in self.data:
            self.data[video_id]["status"] = "downloaded"
            self.data[video_id]["local_file"] = local_file
            if title:
                self.data[video_id]["title"] = title

    def set_unavailable(self, video_id):
        if video_id in self.data:
            self.data[video_id]["status"] = "unavailable"

    def set_failed(self, video_id):
        if video_id in self.data:
            self.data[video_id]["status"] = "failed"

    def clear_failed(self):
        for vid, entry in self.data.items():
            if entry["status"] == "failed":
                entry["status"] = "pending"

    def get_primary_section(self, video_id):
        """Return the first section where this video was found."""
        if video_id in self.data and self.data[video_id]["sections"]:
            return self.data[video_id]["sections"][0]
        return None

    def stats_for_section(self, section):
        """Return (total, downloaded, unavailable, failed, pending) for a section."""
        total = dl = na = fa = pend = 0
        for vid, entry in self.data.items():
            if section in entry["sections"]:
                total += 1
                s = entry["status"]
                if s == "downloaded":
                    dl += 1
                elif s == "unavailable":
                    na += 1
                elif s == "failed":
                    fa += 1
                else:
                    pend += 1
        return total, dl, na, fa, pend


# ─── Scanner ─────────────────────────────────────────────────────────────────

class YouTubeScanner:

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

        # Regex on raw HTML
        for pattern in YT_PATTERNS:
            for m in pattern.finditer(content):
                vid = extract_video_id(m.group(0))
                if vid:
                    video_ids.add(vid)

        # BeautifulSoup
        try:
            soup = BeautifulSoup(content, "lxml")

            for a in soup.find_all("a", href=True):
                vid = extract_video_id(a["href"])
                if vid:
                    video_ids.add(vid)

            for iframe in soup.find_all("iframe", src=True):
                vid = extract_video_id(iframe["src"])
                if vid:
                    video_ids.add(vid)

            for tag in soup.find_all(["embed", "object", "source"]):
                for attr in ("src", "data", "value"):
                    val = tag.get(attr, "")
                    vid = extract_video_id(val)
                    if vid:
                        video_ids.add(vid)

            for param in soup.find_all("param"):
                vid = extract_video_id(param.get("value", ""))
                if vid:
                    video_ids.add(vid)
        except Exception:
            pass

        return video_ids

    def scan_section(self, section_name, index):
        """Scan all HTML files in a section and register in VideoIndex."""
        section_dir = os.path.join(self.site_dir, safe_filename(section_name))
        if not os.path.isdir(section_dir):
            return 0

        count = 0
        for root, dirs, files in os.walk(section_dir):
            for fname in files:
                if not fname.endswith((".html", ".htm")):
                    continue
                fpath = os.path.join(root, fname)
                rel_path = os.path.relpath(fpath, self.site_dir)
                ids = self.scan_file(fpath)
                for vid in ids:
                    index.add_video(vid, section_name, rel_path)
                    count += 1

        return count

    def scan_all(self, index, section_filter=None):
        """Scan all sections and populate the VideoIndex."""
        sections = section_filter if section_filter else SECTIONS
        total = 0
        for sec in sections:
            n = self.scan_section(sec, index)
            total += n
        return total


# ─── Downloader ──────────────────────────────────────────────────────────────

class YouTubeDownloader:

    def __init__(self, video_dir, yt_format, rate_limit=None):
        self.video_dir = video_dir
        self.yt_format = yt_format
        self.rate_limit = rate_limit

    def download(self, video_id, output_dir):
        """
        Download a single video into output_dir.
        Returns: ('ok', local_path, title) | ('exists', ...) |
                 ('unavailable', None, None) | ('error', None, None)
        """
        url = canonical_yt_url(video_id)
        os.makedirs(output_dir, exist_ok=True)

        outtmpl = os.path.join(output_dir, "%(id)s - %(title).80s.%(ext)s")

        cmd = [
            "yt-dlp",
            "--no-playlist",
            "--no-overwrites",
            "--write-thumbnail",
            "--convert-thumbnails", "jpg",
            "--embed-thumbnail",
            "--add-metadata",
            "--print", "after_move:filepath",
            "--print", "%(title)s",
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
                cmd, capture_output=True, text=True, timeout=300,
            )

            stdout = result.stdout.strip()
            stderr = result.stderr

            if result.returncode == 0:
                # Parse printed filepath and title
                lines = stdout.split("\n")
                title = None
                local_file = None

                if len(lines) >= 2:
                    # --print outputs: first the filepath, then the title
                    # But order depends on yt-dlp version, be flexible
                    for line in lines:
                        line = line.strip()
                        if not line:
                            continue
                        if os.path.sep in line or line.endswith(
                            (".mp4", ".mkv", ".webm", ".mp3", ".m4a", ".opus")
                        ):
                            local_file = line
                        elif not title:
                            title = line

                # Fallback: find the file by glob
                if not local_file:
                    pattern = os.path.join(output_dir, f"{video_id} - *")
                    matches = glob.glob(pattern)
                    # Exclude thumbnails
                    matches = [m for m in matches if not m.endswith((".jpg", ".png", ".webp"))]
                    if matches:
                        local_file = matches[0]

                if "has already been downloaded" in stdout + stderr:
                    return "exists", local_file, title

                return "ok", local_file, title
            else:
                combined = stdout + stderr
                unavailable_msgs = [
                    "Video unavailable", "Private video",
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
                for msg in unavailable_msgs:
                    if msg.lower() in combined.lower():
                        return "unavailable", None, None

                return "error", None, None

        except subprocess.TimeoutExpired:
            return "error", None, None
        except Exception as e:
            log.warning(f"      Exception: {e}")
            return "error", None, None


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(
        description="Extract YouTube links from scraped forum pages and download them."
    )
    p.add_argument("--site-dir", default=DEFAULT_SITE_DIR,
                   help="Path to scraped site (default: ./site_mirror)")
    p.add_argument("--video-dir", default=DEFAULT_VIDEO_DIR,
                   help="Where to save videos (default: ./videos)")
    p.add_argument("--format", default=DEFAULT_FORMAT,
                   help="yt-dlp format string")
    p.add_argument("--rate-limit", default=None,
                   help="Download rate limit (e.g. 1M, 500K)")
    p.add_argument("--sections", default=None,
                   help="Comma-separated section indices")
    p.add_argument("--max-per-section", type=int, default=None,
                   help="Max videos to download per section")
    p.add_argument("--scan-only", action="store_true",
                   help="Only scan and report, don't download")
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
        print("Run the forum scraper first.")
        sys.exit(1)

    section_filter = None
    if args.sections is not None:
        indices = [int(x.strip()) for x in args.sections.split(",")]
        section_filter = [SECTIONS[i] for i in indices]

    # ── Setup ──
    print()
    print("=" * 70)
    print("  YouTube Link Extractor & Downloader")
    print(f"  Site dir:   {os.path.abspath(args.site_dir)}")
    print(f"  Video dir:  {os.path.abspath(args.video_dir)}")
    print(f"  Format:     {args.format}")
    print(f"  Scan only:  {'yes' if args.scan_only else 'no'}")
    print("=" * 70)
    print()

    index = VideoIndex(args.video_dir)
    index.load()

    if args.retry_failed:
        index.clear_failed()
        index.save()
        log.info("  Cleared failed status — will retry those videos.\n")

    # ── Phase 1: Scan ──
    log.info("🔍 Phase 1: Scanning HTML files for YouTube links...\n")

    scanner = YouTubeScanner(args.site_dir)
    scanner.scan_all(index, section_filter)
    index.save()

    # Display scan results
    sections_to_show = section_filter if section_filter else SECTIONS
    grand_total_unique = set()

    print(f"  {'Section':<40} {'Videos':>7}")
    print(f"  {'─'*40} {'─'*7}")

    for sec in sections_to_show:
        total, dl, na, fa, pend = index.stats_for_section(sec)
        if total > 0:
            print(f"  📁 {sec:<38} {total:>7}")
        else:
            print(f"     {sec:<38} {total:>7}")
        for vid, entry in index.data.items():
            if sec in entry["sections"]:
                grand_total_unique.add(vid)

    print(f"  {'─'*40} {'─'*7}")
    print(f"  {'UNIQUE VIDEOS':<40} {len(grand_total_unique):>7}")
    print()

    if args.scan_only:
        # Detailed output per section
        for sec in sections_to_show:
            vids_in_sec = [
                (vid, entry) for vid, entry in index.data.items()
                if sec in entry["sections"]
            ]
            if not vids_in_sec:
                continue
            print(f"\n{'─' * 70}")
            print(f"  {sec} ({len(vids_in_sec)} videos)")
            print(f"{'─' * 70}")
            for vid, entry in sorted(vids_in_sec, key=lambda x: x[0]):
                status_icon = {
                    "downloaded": "✓", "unavailable": "⊘",
                    "failed": "✗", "pending": "·",
                }.get(entry["status"], "?")
                print(f"  {status_icon} {entry['url']}")
                for pg in entry["source_pages"][:3]:
                    print(f"    └─ {pg}")
                if len(entry["source_pages"]) > 3:
                    print(f"    └─ ... and {len(entry['source_pages'])-3} more")

        # Save index even in scan-only mode
        index.save()
        log.info(f"\n  Index saved to {index.filepath}")
        sys.exit(0)

    # ── Phase 2: Download ──
    log.info("📥 Phase 2: Downloading videos...\n")

    downloader = YouTubeDownloader(args.video_dir, args.format, args.rate_limit)

    total_ok = 0
    total_skip = 0
    total_unavail = 0
    total_err = 0

    for sec in sections_to_show:
        # Get all videos for this section
        vids_in_sec = [
            vid for vid, entry in index.data.items()
            if sec in entry["sections"]
        ]
        if not vids_in_sec:
            continue

        if args.max_per_section:
            vids_in_sec = vids_in_sec[:args.max_per_section]

        # Section video folder: videos/<Section Name>/
        section_video_dir = os.path.join(args.video_dir, safe_filename(sec))

        already_done = sum(1 for v in vids_in_sec if index.is_done(v))
        to_do = len(vids_in_sec) - already_done

        print(f"{'─' * 70}")
        print(f"  📁 {sec}")
        print(f"     {len(vids_in_sec)} videos ({already_done} done, {to_do} remaining)")
        print(f"{'─' * 70}")

        sec_ok = 0
        sec_skip = 0
        sec_unavail = 0
        sec_err = 0

        for i, vid in enumerate(vids_in_sec, 1):
            if index.is_done(vid):
                sec_skip += 1
                continue

            pct = i / len(vids_in_sec) * 100
            log.info(f"    [{i}/{len(vids_in_sec)}] ({pct:.0f}%) {canonical_yt_url(vid)}")

            status, local_file, title = downloader.download(vid, section_video_dir)

            if status == "ok":
                rel_path = os.path.relpath(local_file, ".") if local_file else None
                index.set_downloaded(vid, rel_path, title)
                log.info(f"      ✓ Downloaded: {os.path.basename(local_file or '')}")
                sec_ok += 1
            elif status == "exists":
                if not index.is_done(vid):
                    rel_path = os.path.relpath(local_file, ".") if local_file else None
                    index.set_downloaded(vid, rel_path, title)
                sec_skip += 1
            elif status == "unavailable":
                index.set_unavailable(vid)
                log.info(f"      ⊘ Unavailable (removed/private)")
                sec_unavail += 1
            else:
                index.set_failed(vid)
                log.info(f"      ✗ Failed")
                sec_err += 1

            index.save()

            if status == "ok":
                time.sleep(1)

        total_ok += sec_ok
        total_skip += sec_skip
        total_unavail += sec_unavail
        total_err += sec_err

        log.info(
            f"  ✅ {sec}: {sec_ok} new, {sec_skip} skipped, "
            f"{sec_unavail} unavailable, {sec_err} failed"
        )

    index.save()

    # ── Summary ──
    print()
    print("=" * 70)
    print("  DOWNLOAD SUMMARY")
    print("=" * 70)
    print(f"  Downloaded:   {total_ok}")
    print(f"  Skipped:      {total_skip} (already done)")
    print(f"  Unavailable:  {total_unavail} (removed/private)")
    print(f"  Failed:       {total_err} (use --retry-failed)")
    print()

    print(f"  {'Section':<40} {'DL':>4} {'Skip':>5} {'N/A':>5} {'Err':>4}")
    print(f"  {'─'*40} {'─'*4} {'─'*5} {'─'*5} {'─'*4}")

    for sec in sections_to_show:
        total, dl, na, fa, pend = index.stats_for_section(sec)
        if total == 0:
            continue

        bar_len = 12
        done = dl + na
        filled = int(bar_len * done / total) if total > 0 else 0
        bar = "█" * filled + "░" * (bar_len - filled)

        print(f"  {sec:<40} {dl:>4} {pend:>5} {na:>5} {fa:>4}  {bar}")

    print()
    print(f"  Video index:  {os.path.abspath(index.filepath)}")
    print(f"  Videos dir:   {os.path.abspath(args.video_dir)}")
    print(f"  Run again to resume. Use --retry-failed to retry errors.")
    print("=" * 70)


if __name__ == "__main__":
    main()