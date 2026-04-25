#!/usr/bin/env python3
"""
YTP Backup — YouTube Downloader (Interactive)
=============================================
Scans YTP nostrane / YTP fai da te forum pages for YouTube links,
fetches video description + channel info from YouTube, then downloads.

Requirements:
    pip install yt-dlp beautifulsoup4 lxml
"""

import os
import re
import sys
import json
import time
import glob
import shutil
import subprocess
import argparse
from pathlib import Path

from bs4 import BeautifulSoup

# ── Sections to scan ────────────────────────"Risorse","Old sources","Tutorial per il pooping"──────────────────────────────────

SCAN_SECTIONS = ["YTP nostrane", "YTP fai da te","YTPMV dimportazione","YTP da internet"] ##"Risorse","Old sources","Tutorial per il pooping

CHANNEL_KEYWORDS = re.compile(
    r'(?i)(YTP|YTPMV|Collab|Youtube\s+poop|YT\s+Poop|Poop'
    r'|matteo\s+montesi|avventure|Zeb|Collegio|Harry potter|Peppa|Grylls|Tennis|Acid|Favij|Testoh|Pingu'
    r'|Dipr[eè]|Bello\s+Figo|Yotobi|He[\s-]?Man|Berlusconi|Muniz|Fabri|Nemesis|Testo|Jack Black|Super Quark|Iscritti|YTM|YTG|MLG)'
)

DEFAULT_SITE_DIR = "./site_mirror"
DEFAULT_VIDEO_DIR = "./videos"
DEFAULT_FORMAT = "bestvideo[height<=720]+bestaudio/best[height<=720]/best"

# ── YouTube URL helpers ───────────────────────────────────────────────────────

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

UNAVAIL_MSGS = [
    "video unavailable", "private video", "has been removed",
    "content is not available", "copyright claim",
    "account associated with this video has been terminated",
    "violates youtube's terms of service", "been removed by the uploader",
    "confirm your age", "join this channel", "members-only",
    "not available in your country", "no longer available",
]

DL_PROGRESS_RE = re.compile(
    r'\[download\]\s+([\d.]+)%\s+of\s+~?([\d.]+\s*\w+)\s+at\s+([\d.]+\s*[\w/]+)'
)


def extract_video_id(url):
    m = YT_ID_RE.search(url)
    return m.group(1) if m else None


def canonical_yt_url(vid):
    return f"https://www.youtube.com/watch?v={vid}"


def channel_videos_url(channel_url):
    url = channel_url.rstrip("/")
    url = re.sub(r'/(videos|shorts|streams|playlists|about|community|featured)$', '', url)
    return url + "/videos"


def safe_filename(name, max_len=80):
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name[:max_len]


def thread_title_from_filename(fname):
    """'71236585_Some Thread Title.html'  →  'Some Thread Title'"""
    stem = Path(fname).stem
    m = re.match(r'^\d+_(.*)', stem)
    return m.group(1) if m else stem


def bar(pct, width=28):
    filled = int(width * pct / 100)
    return "[" + "=" * filled + " " * (width - filled) + f"] {pct:5.1f}%"


def clear_line():
    cols = shutil.get_terminal_size((80, 24)).columns
    print("\r" + " " * cols + "\r", end="", flush=True)


# ── Video Index ───────────────────────────────────────────────────────────────

class VideoIndex:
    """
    {
      "VIDEO_ID": {
        "url":          "https://www.youtube.com/watch?v=...",
        "title":         str | null,
        "description":   str | null,        ← from YouTube
        "channel_name":  str | null,        ← from YouTube
        "channel_url":   str | null,        ← from YouTube
        "sections":      ["YTP nostrane", ...],
        "source_pages":  ["YTP nostrane/71236585_Title.html", ...],
        "thread_titles": ["In the Madonna — Tassista Romano", ...],
        "status":        "pending" | "downloaded" | "unavailable" | "failed",
        "local_file":    str | null
      }
    }
    """

    def __init__(self, video_dir):
        self.video_dir = video_dir
        self.filepath = os.path.join(video_dir, "video_index.json")
        self.data = {}

    def load(self):
        if os.path.exists(self.filepath):
            with open(self.filepath, encoding="utf-8") as f:
                self.data = json.load(f)

    def save(self):
        os.makedirs(self.video_dir, exist_ok=True)
        with open(self.filepath, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)

    def add_video(self, video_id, section, source_page, thread_title=None):
        if video_id not in self.data:
            self.data[video_id] = {
                "url": canonical_yt_url(video_id),
                "title": None,
                "description": None,
                "channel_name": None,
                "channel_url": None,
                "publish_date": None,
                "view_count": None,
                "like_count": None,
                "sections": [],
                "source_pages": [],
                "thread_titles": [],
                "status": "pending",
                "local_file": None,
            }
        e = self.data[video_id]
        if section not in e["sections"]:
            e["sections"].append(section)
        if source_page not in e["source_pages"]:
            e["source_pages"].append(source_page)
        if thread_title and thread_title not in e.get("thread_titles", []):
            e.setdefault("thread_titles", []).append(thread_title)

    def needs_metadata(self, video_id):
        e = self.data.get(video_id, {})
        if e.get("status") == "unavailable":
            return False
        if e.get("title") == "warnings.warn(":
            return True
        return (e.get("title") is None or
                e.get("description") is None or
                e.get("channel_name") is None or
                e.get("view_count") is None)

    def set_metadata(self, video_id, title=None, description=None,
                     channel_name=None, channel_url=None,
                     publish_date=None, view_count=None, like_count=None):
        if video_id not in self.data:
            return
        e = self.data[video_id]
        if title:
            e["title"] = title
        if description is not None:
            e["description"] = description
        if channel_name:
            e["channel_name"] = channel_name
        if channel_url:
            e["channel_url"] = channel_url
        if publish_date is not None:
            e["publish_date"] = publish_date
        if view_count is not None:
            e["view_count"] = view_count
        if like_count is not None:
            e["like_count"] = like_count

    def is_done(self, vid):
        return self.data.get(vid, {}).get("status") in ("downloaded", "unavailable")

    def set_downloaded(self, vid, local_file, title=None):
        if vid in self.data:
            e = self.data[vid]
            e["status"] = "downloaded"
            e["local_file"] = local_file
            if title:
                e["title"] = title

    def set_unavailable(self, vid):
        if vid in self.data:
            self.data[vid]["status"] = "unavailable"

    def set_failed(self, vid):
        if vid in self.data:
            self.data[vid]["status"] = "failed"

    def clear_failed(self):
        for e in self.data.values():
            if e["status"] == "failed":
                e["status"] = "pending"

    def pending(self):
        return [vid for vid, e in self.data.items() if e["status"] == "pending"]

    def stats(self):
        s = {"total": 0, "downloaded": 0, "unavailable": 0, "failed": 0, "pending": 0}
        for e in self.data.values():
            s["total"] += 1
            key = e.get("status", "pending")
            s[key] = s.get(key, 0) + 1
        return s


# ── Scan Cache ───────────────────────────────────────────────────────────────

class ScanCache:
    """
    Tracks which HTML pages have already been scanned.
    Stored separately from video_index.json so it can be inspected / cleared.
    {
      "rel/path/to/page.html": {
        "scanned_at": "2025-01-01T00:00:00",
        "video_ids":  ["id1", "id2"],
        "new_count":  2
      }
    }
    """

    def __init__(self, video_dir):
        self.filepath = os.path.join(video_dir, "scan_cache.json")
        self.data = {}

    def load(self):
        if os.path.exists(self.filepath):
            with open(self.filepath, encoding="utf-8") as f:
                self.data = json.load(f)

    def save(self):
        os.makedirs(os.path.dirname(self.filepath), exist_ok=True)
        with open(self.filepath, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)

    def is_scanned(self, rel_path):
        return rel_path in self.data

    def mark_scanned(self, rel_path, video_ids, new_count):
        import datetime
        self.data[rel_path] = {
            "scanned_at": datetime.datetime.now().isoformat(timespec="seconds"),
            "video_ids":  list(video_ids),
            "new_count":  new_count,
        }


# ── Scanner ───────────────────────────────────────────────────────────────────

class Scanner:

    def __init__(self, site_dir):
        self.site_dir = site_dir

    def scan_file(self, filepath):
        try:
            content = Path(filepath).read_text(encoding="utf-8", errors="replace")
        except Exception:
            return set()
        ids = set()
        for pat in YT_PATTERNS:
            for m in pat.finditer(content):
                vid = extract_video_id(m.group(0))
                if vid:
                    ids.add(vid)
        try:
            soup = BeautifulSoup(content, "lxml")
            for tag in soup.find_all(["a", "iframe", "embed", "object", "source", "param"]):
                for attr in ("href", "src", "data", "value"):
                    vid = extract_video_id(tag.get(attr, ""))
                    if vid:
                        ids.add(vid)
        except Exception:
            pass
        return ids

    def scan_sections(self, index, scan_cache=None, save_fn=None, save_interval=10):
        new_found = 0
        file_count = 0
        skipped = 0
        for sec in SCAN_SECTIONS:
            sec_dir = os.path.join(self.site_dir, sec)
            if not os.path.isdir(sec_dir):
                print(f"  [!] Directory not found: {sec_dir}")
                continue
            html_files = []
            for root, _, files in os.walk(sec_dir):
                for fname in files:
                    if fname.endswith((".html", ".htm")):
                        html_files.append(os.path.join(root, fname))

            print(f"  {sec}: {len(html_files)} HTML files", flush=True)
            for fpath in html_files:
                rel = os.path.relpath(fpath, self.site_dir)

                if scan_cache and scan_cache.is_scanned(rel):
                    skipped += 1
                    continue

                fname = os.path.basename(fpath)
                if re.match(r'^page_\d+\.html$', fname):
                    parent = os.path.basename(os.path.dirname(fpath))
                    thread_title = thread_title_from_filename(parent)
                else:
                    thread_title = thread_title_from_filename(fname)

                ids = self.scan_file(fpath)
                new_this_file = 0
                for vid in ids:
                    was_new = vid not in index.data
                    index.add_video(vid, sec, rel, thread_title)
                    if was_new:
                        new_found += 1
                        new_this_file += 1

                if scan_cache:
                    scan_cache.mark_scanned(rel, ids, new_this_file)

                if ids:
                    print(f"  [scan] {rel}  → {len(ids)} video(s) found, {new_this_file} new", flush=True)
                else:
                    print(f"  [scan] {rel}  → no videos", flush=True)

                file_count += 1
                if save_fn and file_count % save_interval == 0:
                    save_fn()
                    if scan_cache:
                        scan_cache.save()

        if skipped:
            print(f"  (skipped {skipped} already-scanned pages)")
        return new_found


# ── YouTube metadata ──────────────────────────────────────────────────────────

def fetch_yt_metadata(video_id):
    """
    Run yt-dlp --dump-json to get title, description, channel info.
    Returns dict | 'unavailable' | None (temp error)
    """
    url = canonical_yt_url(video_id)
    try:
        r = subprocess.run(
            ["yt-dlp", "--dump-json", "--no-playlist",
             "--socket-timeout", "20", url],
            capture_output=True, text=True, timeout=60,
        )
        if r.returncode == 0 and r.stdout.strip():
            raw = next(
                (l for l in reversed(r.stdout.splitlines()) if l.strip().startswith("{")),
                None,
            )
            if raw is None:
                return None
            d = json.loads(raw)
            raw_date = d.get("upload_date")  # "20230415"
            publish_date = None
            if raw_date and len(raw_date) == 8:
                publish_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"
            return {
                "title":        d.get("title"),
                "description":  (d.get("description") or "")[:3000],
                "channel_name": d.get("uploader") or d.get("channel"),
                "channel_url":  d.get("uploader_url") or d.get("channel_url"),
                "publish_date": publish_date,
                "view_count":   d.get("view_count"),
                "like_count":   d.get("like_count"),
            }
        combined = (r.stdout + r.stderr).lower()
        for msg in UNAVAIL_MSGS:
            if msg in combined:
                return "unavailable"
    except Exception:
        pass
    return None


# ── Downloader ────────────────────────────────────────────────────────────────

def download_video(video_id, output_dir, yt_format, rate_limit,
                   current_num, total_num):
    """
    Download one video with a real-time per-video progress bar.
    Shows:  Video:   [=====     ] 45.2%  23.4MB @ 1.5MB/s
            Overall: [==        ] 12/80
    Returns ('ok'|'exists'|'unavailable'|'error', local_file, title)
    """
    url = canonical_yt_url(video_id)
    os.makedirs(output_dir, exist_ok=True)
    outtmpl = os.path.join(output_dir, "%(title).80s - %(id)s.%(ext)s")

    cmd = [
        "yt-dlp",
        "--no-playlist", "--no-overwrites",
        "--write-thumbnail", "--convert-thumbnails", "jpg",
        "--embed-thumbnail", "--add-metadata",
        "--newline",
        "--print", "after_move:filepath",
        "--print", "%(title)s",
        "--format", yt_format,
        "--output", outtmpl,
        "--retries", "3",
        "--socket-timeout", "30",
        "--no-warnings",
    ]
    if rate_limit:
        cmd += ["--limit-rate", rate_limit]
    cmd.append(url)

    local_file = None
    title = None
    is_exists = False

    overall_pct = current_num / total_num * 100
    ov_bar = bar(overall_pct, 24)

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        for line in proc.stdout:
            line = line.rstrip()
            if not line:
                continue

            if "has already been downloaded" in line:
                is_exists = True
                continue

            m = DL_PROGRESS_RE.search(line)
            if m:
                vid_pct = float(m.group(1))
                size = m.group(2).strip()
                speed = m.group(3).strip()
                vid_bar = bar(vid_pct, 24)
                print(
                    f"\r  Video:   {vid_bar}  {size} @ {speed}    ",
                    end="", flush=True,
                )
                continue

            # --print outputs (filepath / title) arrive after download
            stripped = line.strip()
            # skip Python warnings: "path/to/file.py:123: SomeWarning: msg"
            if re.match(r'.+\.py:\d+: \w+Warning:', stripped):
                continue
            if stripped and not stripped.startswith("["):
                if (os.sep in stripped or "/" in stripped) and any(
                    stripped.endswith(e)
                    for e in (".mp4", ".mkv", ".webm", ".mp3", ".m4a", ".opus")
                ):
                    local_file = stripped
                elif not title:
                    title = stripped

        proc.wait()
        clear_line()

        if not local_file:
            matches = [
                m for m in glob.glob(os.path.join(output_dir, f"* - {video_id}.*"))
                if not m.endswith((".jpg", ".png", ".webp"))
            ]
            if matches:
                local_file = matches[0]

        if is_exists:
            return "exists", local_file, title
        if proc.returncode == 0:
            return "ok", local_file, title

        return "error", None, None

    except subprocess.TimeoutExpired:
        proc.kill()
        clear_line()
        return "error", None, None
    except Exception as ex:
        clear_line()
        print(f"  [!] {ex}")
        return "error", None, None


# ── Interactive phases ────────────────────────────────────────────────────────

def do_update_index(index, site_dir):
    scanner = Scanner(site_dir)
    scan_cache = ScanCache(index.video_dir)
    scan_cache.load()
    cached_count = len(scan_cache.data)
    if cached_count:
        print(f"  Scan cache: {cached_count} pages already processed — will skip them.")

    print("  Scanning HTML pages for YouTube links...")
    new_count = scanner.scan_sections(index, scan_cache=scan_cache, save_fn=index.save)
    index.save()
    scan_cache.save()
    print(f"  Scan cache saved → {os.path.abspath(scan_cache.filepath)}")

    st = index.stats()
    print(f"  Total videos in index: {st['total']}  (new this run: {new_count})")
    print()

    need_meta = [
        vid for vid in index.data
        if index.needs_metadata(vid)
    ]

    if not need_meta:
        print("  All videos already have metadata.")
        return

    total_meta = len(need_meta)
    print(f"  Fetching YouTube metadata for {total_meta} videos")
    print(f"  (title, description, channel link)...")
    print()

    for i, vid in enumerate(need_meta, 1):
        overall_pct = i / total_meta * 100
        ov_bar = bar(overall_pct, 30)
        print(f"\r  {ov_bar}  {i}/{total_meta}", end="", flush=True)

        meta = fetch_yt_metadata(vid)
        if meta == "unavailable":
            index.set_unavailable(vid)
        elif meta:
            index.set_metadata(vid, **meta)
        # None = temp error, leave as-is for next run

        if i % 20 == 0:
            index.save()

    clear_line()
    index.save()

    st = index.stats()
    print(f"  Done — index updated.")
    print(f"  Total: {st['total']}  Pending: {st['pending']}  "
          f"Unavailable: {st['unavailable']}")


def do_download(index, video_dir, yt_format, rate_limit, retry_failed):
    if retry_failed:
        index.clear_failed()
        index.save()
        print("  Cleared failed status — will retry.\n")

    pending = index.pending()
    if not pending:
        print("  Nothing to download — either run 'Update index' first")
        print("  or everything is already downloaded / unavailable.")
        return

    total = len(pending)
    print(f"  {total} videos pending.\n")

    ok_count = skip_count = unavail_count = err_count = 0

    for i, vid in enumerate(pending, 1):
        e = index.data[vid]
        sec = e["sections"][0] if e["sections"] else "Unknown"
        thread = (e.get("thread_titles") or [""])[0] or vid
        yt_title = e.get("title") or vid

        out_dir = os.path.join(video_dir, safe_filename(sec))

        # ── header ──
        print(f"  [{i}/{total}] {thread[:60]}")
        if e.get("channel_name"):
            ch_url = e.get("channel_url", "")
            print(f"  Channel: {e['channel_name']}  {ch_url}")
        print(f"  URL:     {canonical_yt_url(vid)}")

        # ── download + per-video bar ──
        status, local_file, dl_title = download_video(
            vid, out_dir, yt_format, rate_limit, i, total,
        )

        # ── result ──
        if status == "ok":
            rel = os.path.relpath(local_file, ".") if local_file else None
            index.set_downloaded(vid, rel, dl_title)
            print(f"  ✓ {os.path.basename(local_file or '')}")
            ok_count += 1
        elif status == "exists":
            if not index.is_done(vid):
                rel = os.path.relpath(local_file, ".") if local_file else None
                index.set_downloaded(vid, rel, dl_title)
            print(f"  = already downloaded")
            skip_count += 1
        elif status == "unavailable":
            index.set_unavailable(vid)
            print("  ⊘ Unavailable (removed / private)")
            unavail_count += 1
        else:
            index.set_failed(vid)
            print("  ✗ Failed")
            err_count += 1

        index.save()

        # ── overall bar ──
        done = ok_count + skip_count + unavail_count + err_count
        ov_pct = done / total * 100
        ov_bar = bar(ov_pct, 30)
        print(f"  Overall: {ov_bar}  {done}/{total}  "
              f"dl={ok_count} skip={skip_count} err={err_count}")
        print()

        if status == "ok":
            time.sleep(1)

    print("─" * 54)
    print(f"  Downloaded:  {ok_count}")
    print(f"  Skipped:     {skip_count}  (already on disk)")
    print(f"  Unavailable: {unavail_count}  (removed / private)")
    print(f"  Failed:      {err_count}  (re-run to retry)")
    print(f"  Index:       {os.path.abspath(index.filepath)}")


def do_scrape_channels(index):
    channel_urls = {}
    for e in index.data.values():
        url = e.get("channel_url")
        if url and url not in channel_urls:
            channel_urls[url] = e.get("channel_name") or url

    if not channel_urls:
        print("  No channels found in index. Run 'Update index' first.")
        return

    print(f"  Found {len(channel_urls)} unique channel(s).")
    new_total = 0

    for ch_url, ch_name in channel_urls.items():
        print(f"\n  Scraping: {ch_name}")
        videos_url = channel_videos_url(ch_url)

        try:
            r = subprocess.run(
                ["yt-dlp", "--flat-playlist", "--dump-json",
                 "--no-warnings", "--socket-timeout", "30", videos_url],
                capture_output=True, text=True, timeout=300,
            )
            lines = [l for l in r.stdout.splitlines() if l.strip().startswith("{")]
            print(f"  {len(lines)} videos found on channel.")

            new_count = 0
            for line in lines:
                try:
                    d = json.loads(line)
                    vid_id = d.get("id")
                    title = d.get("title") or ""
                    if not vid_id:
                        continue
                    if not CHANNEL_KEYWORDS.search(title):
                        continue
                    was_new = vid_id not in index.data
                    index.add_video(vid_id, "Youtube", f"channel_scrape:{ch_url}", title)
                    e = index.data[vid_id]
                    if not e.get("title"):
                        index.set_metadata(vid_id, title=title,
                                           channel_name=ch_name, channel_url=ch_url)
                    if was_new:
                        new_count += 1
                        new_total += 1
                except (json.JSONDecodeError, KeyError):
                    continue

            print(f"  Matched: {new_count} new YTP-related video(s) added to 'Youtube' section.")
        except subprocess.TimeoutExpired:
            print(f"  [!] Timeout scraping {ch_url}")
        except Exception as ex:
            print(f"  [!] Error: {ex}")

        index.save()

    print(f"\n  Done. {new_total} new video(s) added to 'Youtube' section.")
    st = index.stats()
    youtube_total = sum(
        1 for e in index.data.values() if "Youtube" in e.get("sections", [])
    )
    print(f"  Total videos in 'Youtube' section: {youtube_total}  (index total: {st['total']})")


def do_download_youtube(index, video_dir, yt_format, rate_limit, retry_failed):
    if retry_failed:
        for e in index.data.values():
            if "Youtube" in e.get("sections", []) and e["status"] == "failed":
                e["status"] = "pending"
        index.save()
        print("  Cleared failed status for 'Youtube' section — will retry.\n")

    pending = [
        vid for vid, e in index.data.items()
        if "Youtube" in e.get("sections", []) and e["status"] == "pending"
    ]

    if not pending:
        print("  Nothing to download in 'Youtube' section.")
        print("  Run 'Scrape channels' first, or everything is already downloaded.")
        return

    total = len(pending)
    print(f"  {total} 'Youtube' section video(s) pending.\n")

    ok_count = skip_count = unavail_count = err_count = 0
    out_dir = os.path.join(video_dir, "Youtube")

    for i, vid in enumerate(pending, 1):
        e = index.data[vid]
        label = (e.get("thread_titles") or [""])[0] or e.get("title") or vid

        print(f"  [{i}/{total}] {label[:60]}")
        if e.get("channel_name"):
            print(f"  Channel: {e['channel_name']}  {e.get('channel_url', '')}")
        print(f"  URL:     {canonical_yt_url(vid)}")

        status, local_file, dl_title = download_video(
            vid, out_dir, yt_format, rate_limit, i, total,
        )

        if status == "ok":
            rel = os.path.relpath(local_file, ".") if local_file else None
            index.set_downloaded(vid, rel, dl_title)
            print(f"  ✓ {os.path.basename(local_file or '')}")
            ok_count += 1
        elif status == "exists":
            if not index.is_done(vid):
                rel = os.path.relpath(local_file, ".") if local_file else None
                index.set_downloaded(vid, rel, dl_title)
            print(f"  = already downloaded")
            skip_count += 1
        elif status == "unavailable":
            index.set_unavailable(vid)
            print("  ⊘ Unavailable (removed / private)")
            unavail_count += 1
        else:
            index.set_failed(vid)
            print("  ✗ Failed")
            err_count += 1

        index.save()

        done = ok_count + skip_count + unavail_count + err_count
        ov_pct = done / total * 100
        ov_bar = bar(ov_pct, 30)
        print(f"  Overall: {ov_bar}  {done}/{total}  "
              f"dl={ok_count} skip={skip_count} err={err_count}")
        print()

        if status == "ok":
            time.sleep(1)

    print("─" * 54)
    print(f"  Downloaded:  {ok_count}")
    print(f"  Skipped:     {skip_count}  (already on disk)")
    print(f"  Unavailable: {unavail_count}  (removed / private)")
    print(f"  Failed:      {err_count}  (re-run to retry)")
    print(f"  Index:       {os.path.abspath(index.filepath)}")


def do_stats(index):
    from collections import defaultdict

    if not index.data:
        print("  Index is empty. Run 'Update index' first.")
        return

    channels = defaultdict(lambda: {"total": 0, "downloaded": 0, "unavailable": 0,
                                     "pending": 0, "failed": 0, "sections": set()})

    for e in index.data.values():
        name = e.get("channel_name") or "(unknown)"
        ch = channels[name]
        ch["total"] += 1
        ch[e.get("status", "pending")] += 1
        for s in e.get("sections", []):
            ch["sections"].add(s)

    rows = sorted(channels.items(), key=lambda x: x[1]["total"], reverse=True)

    col_ch  = max(len("Channel"), max(len(n) for n, _ in rows))
    col_ch  = min(col_ch, 40)

    header = (f"  {'Channel':<{col_ch}}  {'Total':>5}  {'DL':>4}  "
              f"{'Pend':>4}  {'N/A':>4}  {'Fail':>4}  Sections")
    sep    = "  " + "-" * (len(header) - 2)

    print()
    print(header)
    print(sep)
    for name, c in rows:
        truncated = name[:col_ch]
        secs = ", ".join(sorted(c["sections"]))
        print(f"  {truncated:<{col_ch}}  {c['total']:>5}  {c['downloaded']:>4}  "
              f"{c['pending']:>4}  {c['unavailable']:>4}  {c['failed']:>4}  {secs}")

    print(sep)
    totals = index.stats()
    print(f"  {'TOTAL':<{col_ch}}  {totals['total']:>5}  {totals['downloaded']:>4}  "
          f"{totals['pending']:>4}  {totals['unavailable']:>4}  {totals.get('failed', 0):>4}")
    print()


def do_chronology(index, top_n=20):
    if not index.data:
        print("  Index is empty. Run 'Update index' first.")
        return

    candidates = [
        e for e in index.data.values()
        if e.get("status") != "unavailable"
        and e.get("title")
        and e.get("title") != "warnings.warn("
        and e.get("view_count") is not None
    ]

    if not candidates:
        print("  No view count data yet. Run 'Update index' to fetch metadata.")
        return

    top = sorted(candidates, key=lambda e: e.get("view_count") or 0, reverse=True)[:top_n]
    top.sort(key=lambda e: e.get("publish_date") or "")

    col_title   = 40
    col_channel = 22
    header = (f"  {'#':>3}  {'Year':<4}  {'Title':<{col_title}}  "
              f"{'Channel':<{col_channel}}  {'Views':>10}  {'Likes':>8}")
    sep    = "  " + "-" * (len(header) - 2)

    print()
    print(header)
    print(sep)
    for rank, e in enumerate(top, 1):
        year    = (e.get("publish_date") or "????")[:4]
        title   = (e.get("title") or "")[:col_title]
        channel = (e.get("channel_name") or "")[:col_channel]
        views   = e.get("view_count")
        likes   = e.get("like_count")
        views_s = f"{views:,}" if views is not None else "—"
        likes_s = f"{likes:,}" if likes is not None else "—"
        print(f"  {rank:>3}  {year:<4}  {title:<{col_title}}  "
              f"{channel:<{col_channel}}  {views_s:>10}  {likes_s:>8}")
    print(sep)
    print(f"  Top {len(top)} most-viewed videos (of {len(candidates)} with view data), sorted by year")
    print()


def _fmt_views_it(n):
    """Italian-style compact view count: 1,2 mln / 310K / 5 / —"""
    if n is None:
        return "—"
    if n >= 1_000_000_000:
        v = n / 1_000_000_000
        s = f"{v:.1f}".replace(".", ",")
        return f"{s} mrd" if v != int(v) else f"{int(v)} mrd"
    if n >= 1_000_000:
        v = n / 1_000_000
        s = f"{v:.1f}".replace(".", ",")
        return f"{s} mln" if v != int(v) else f"{int(v)} mln"
    if n >= 1_000:
        v = n / 1_000
        s = f"{v:.1f}".replace(".", ",")
        return f"{s}K" if v != int(v) else f"{int(v)}K"
    return str(n)


def do_dump_poopers(index, output_path="poopers.md"):
    from collections import defaultdict

    if not index.data:
        print("  Index is empty. Run 'Update index' first.")
        return

    # Group non-unavailable videos by channel_name
    channels = defaultdict(list)
    for vid, e in index.data.items():
        ch = e.get("channel_name")
        if not ch or e.get("status") == "unavailable":
            continue
        if not e.get("title") or e.get("title") == "warnings.warn(":
            continue
        channels[ch].append((vid, e))

    if not channels:
        print("  No channel data. Run 'Update index' first.")
        return

    def channel_sort_key(item):
        entries = item[1]
        total_views = sum(e.get("view_count") or 0 for _, e in entries)
        return total_views

    sorted_channels = sorted(channels.items(), key=channel_sort_key, reverse=True)

    lines = [
        "| Pooper | Canale | Video | Views totali | Primo | Ultimo | Video più visto | Altri video |",
        "|---|---|---|---|---|---|---|---|",
    ]

    for ch_name, entries in sorted_channels:
        ch_url = next((e.get("channel_url") for _, e in entries if e.get("channel_url")), None)

        by_views = sorted(entries, key=lambda x: x[1].get("view_count") or 0, reverse=True)

        top_vid, top_e = by_views[0]
        top_title = (top_e.get("title") or top_vid).replace("|", "\\|")
        top_url   = top_e.get("url") or f"https://www.youtube.com/watch?v={top_vid}"
        top_views = top_e.get("view_count")
        if top_views is not None:
            top_cell = f"[{top_title}]({top_url}) ({_fmt_views_it(top_views)})"
        else:
            top_cell = f"[{top_title}]({top_url})"

        others = []
        for vid, e in by_views[1:4]:
            t = (e.get("title") or vid).replace("|", "\\|")
            others.append(f'"{t}"')
        others_cell = ", ".join(others)

        video_count  = len(entries)
        total_views  = sum(e.get("view_count") or 0 for _, e in entries)
        views_cell   = _fmt_views_it(total_views) if total_views > 0 else "—"

        dates        = sorted(e.get("publish_date") for _, e in entries if e.get("publish_date"))
        first_cell   = dates[0][:7] if dates else "—"
        last_cell    = dates[-1][:7] if dates else "—"

        canale_cell  = f"[{ch_name}]({ch_url})" if ch_url else ch_name
        ch_safe      = ch_name.replace("|", "\\|")

        lines.append(
            f"| {ch_safe} | {canale_cell} | {video_count} | {views_cell} "
            f"| {first_cell} | {last_cell} | {top_cell} | {others_cell} |"
        )

    content = "\n".join(lines) + "\n"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"  {len(sorted_channels)} poopers → {os.path.abspath(output_path)}")


# ── Menu helpers ──────────────────────────────────────────────────────────────

def ask(prompt, choices):
    while True:
        ans = input(prompt).strip().lower()
        if ans in choices:
            return ans
        print(f"  Please enter one of: {' / '.join(choices)}")


def print_header():
    print()
    print("╔══════════════════════════════════════════════════╗")
    print("║   YTP Backup — YouTube Index & Downloader        ║")
    print("╚══════════════════════════════════════════════════╝")
    print()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    import io
    if hasattr(sys.stdout, 'buffer'):
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

    p = argparse.ArgumentParser(add_help=False)
    p.add_argument("--site-dir",     default=DEFAULT_SITE_DIR)
    p.add_argument("--video-dir",    default=DEFAULT_VIDEO_DIR)
    p.add_argument("--format",       default=DEFAULT_FORMAT)
    p.add_argument("--rate-limit",   default=None)
    p.add_argument("--retry-failed", action="store_true")
    p.add_argument("--stats",        action="store_true",
                   help="Print channel stats table and exit")
    p.add_argument("--chronology",   action="store_true",
                   help="Print top-20 most-viewed videos by year and exit")
    p.add_argument("--dump-poopers", metavar="OUTPUT", nargs="?", const="poopers.md",
                   help="Dump pooper table to Markdown file (default: poopers.md)")
    args, _ = p.parse_known_args()

    if not os.path.isdir(args.site_dir):
        print(f"[!] site_dir not found: {args.site_dir}")
        sys.exit(1)

    if args.stats or args.chronology or args.dump_poopers:
        index = VideoIndex(args.video_dir)
        index.load()
        if args.stats:
            do_stats(index)
        if args.chronology:
            do_chronology(index)
        if args.dump_poopers:
            do_dump_poopers(index, args.dump_poopers)
        return

    print_header()
    print(f"  Site dir:  {os.path.abspath(args.site_dir)}")
    print(f"  Video dir: {os.path.abspath(args.video_dir)}")
    print(f"  Sections:  {', '.join(SCAN_SECTIONS)}")
    print()
    print("  What do you want to do?")
    print()
    print("  1  Update YT video index")
    print("       Scan HTML pages for links, then fetch title /")
    print("       description / channel URL from YouTube.")
    print()
    print("  2  Download indexed videos")
    print("       Download all pending videos in the index.")
    print()
    print("  3  Scrape channels")
    print("       For every channel in the index, fetch all video titles")
    print("       and add YTP / YTPMV / Collab / Youtube poop matches")
    print("       to a new 'Youtube' section.")
    print()
    print("  4  Download 'Youtube' section")
    print("       Download only videos scraped via mode 3.")
    print()
    print("  5  Both  (update index, then download all)")
    print()
    print("  6  Stats")
    print("       Channel table: most videos first.")
    print()
    print("  7  Chronology")
    print("       Top 20 most-viewed videos, sorted by year.")
    print()
    print("  8  Dump poopers")
    print("       Write poopers.md with one row per channel.")
    print()
    print("  q  Quit")
    print()
    choice = ask("  Choice [1/2/3/4/5/6/7/8/q]: ", {"1", "2", "3", "4", "5", "6", "7", "8", "q"})

    if choice == "q":
        sys.exit(0)

    print()

    index = VideoIndex(args.video_dir)
    index.load()

    if choice in ("1", "5"):
        do_update_index(index, args.site_dir)
        print()

    if choice == "3":
        do_scrape_channels(index)
        print()

    if choice in ("2", "5"):
        do_download(
            index,
            args.video_dir,
            args.format,
            args.rate_limit,
            args.retry_failed,
        )

    if choice == "4":
        do_download_youtube(
            index,
            args.video_dir,
            args.format,
            args.rate_limit,
            args.retry_failed,
        )

    if choice == "6":
        do_stats(index)

    if choice == "7":
        do_chronology(index)

    if choice == "8":
        do_dump_poopers(index)

    print()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        import traceback
        print("\n[!] Unexpected error:")
        traceback.print_exc()
    except KeyboardInterrupt:
        print("\n  Interrupted.")
    finally:
        print()
        try:
            if sys.stdin.isatty():
                input("  Press Enter to close...")
        except (EOFError, OSError):
            pass
