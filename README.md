# YTP Italian Forum Backup

Offline scraper and local mirror server for the YouTube Poop Italian Forum
(`youtubepoopita.forumfree.it`).

---

## Structure

```
ytpbackup/
├── scraper.py          # Downloads forum sections, index pages, threads
├── server.js           # Node.js local mirror server
├── package.json
└── site_mirror/
    ├── Home.html
    ├── .scraper_state.json
    ├── Risorse/
    │   ├── index/
    │   │   ├── Risorse.html
    │   │   ├── Risorse - pagina 2.html
    │   │   └── …
    │   ├── 64123456_Thread title.html        ← single-page thread
    │   └── 64123457_Another thread/          ← multi-page thread
    │       ├── page_1.html
    │       └── page_2.html
    └── … (one folder per section)
```
---

## Mirror Server

A zero-dependency Node.js HTTP server that serves the scraped pages with all
internal forum links rewritten to local equivalents.

### Requirements

Node.js ≥ 18 (no `npm install` needed).

### Start

```bash
node server.js          # http://localhost:3000
PORT=8080 node server.js
```

Or via npm:

```bash
npm start
npm run dev             # auto-restart on file changes (Node ≥ 18)
```

### How links are resolved

| Original URL | Served from |
|---|---|
| `https://youtubepoopita.forumfree.it/` | `site_mirror/Home.html` |
| `?f=6350394` | `site_mirror/Risorse/index/Risorse.html` |
| `?f=6350394&st=30` | `site_mirror/Risorse/index/Risorse - pagina 2.html` |
| `?t=64123456` | `site_mirror/Risorse/64123456_Title.html` |
| `?t=64123456&st=30` | `site_mirror/Risorse/64123456_Title/page_2.html` |

Pagination links in the pages (`?f=…&st=…`, `?t=…&st=…`) are already in query-
string format, so the domain-strip rewrite makes them work immediately.

The forum's `page_jump()` function (used by the "jump to page" dialog) is
overridden with a local implementation that navigates to the correct `?st=`
offset.

### On-demand scraping

When you navigate to a page that exists in `.scraper_state.json` but has not
been downloaded yet, the server:

1. Spawns `python scraper.py --sections N --thread-url URL` in the background.
2. Returns a **"Scraping in corso…"** page that auto-refreshes every 10 seconds.
3. Serves the real page once the scraper finishes.

Section index pages not yet downloaded trigger a full section index scrape
(`python scraper.py --sections N`) instead.

Pages for thread IDs that are completely unknown (not in any section's
`threads_found`) return a 404 with a direct link to the live forum.

---

## Scraper

### Requirements

```bash
pip install playwright beautifulsoup4 lxml requests
playwright install chromium
```

### Usage

```bash
# Scrape everything
python scraper.py

# Slower pace
python scraper.py --delay 2.0

# Specific sections only (use --list to see indices)
python scraper.py --sections 0,1,5

# List all sections with their index
python scraper.py --list

# Skip image embedding
python scraper.py --no-embed-images

# Also inline CSS
python scraper.py --embed-css

# Scrape a single thread (used internally by the mirror server)
python scraper.py --sections 3 --thread-url "https://youtubepoopita.forumfree.it/?t=12345678"
```

Run again at any time to resume — already-scraped pages are skipped automatically.

### Scraping passes

| Pass | What happens |
|------|-------------|
| 1    | Downloads `Home.html` |
| 2    | Downloads all section index pages into `{Section}/index/` |
| 2.5  | Scans saved index HTML files for any thread links missed during Pass 2 |
| 3    | Downloads every thread page |

Progress is saved in `site_mirror/.scraper_state.json` after every few threads.

