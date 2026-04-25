#!/usr/bin/env node
/**
 * YTP Forum Mirror Server
 *
 * Serves scraped forum pages from site_mirror/ and rewrites every internal
 * forum link to point at the local mirror instead of the live site.
 * When a page has not been scraped yet it triggers scraper.py for that
 * section/thread and returns a "please wait / auto-refresh" page.
 *
 * Usage:
 *   node server.js              # default port 3000
 *   PORT=8080 node server.js    # custom port
 *
 * No npm packages required — pure Node.js built-ins.
 */

'use strict';

const http  = require('http');
const fs    = require('fs');
const path  = require('path');
const { URL }     = require('url');
const { spawn }   = require('child_process');

// ─── Config ───────────────────────────────────────────────────────────────────
const PORT        = parseInt(process.env.PORT || '3000', 10);
const SITE_MIRROR = path.join(__dirname, 'site_mirror');
const STATE_FILE  = path.join(SITE_MIRROR, '.scraper_state.json');
const SCRAPER     = path.join(__dirname, 'scraper.py');
const BASE_DOMAIN = 'youtubepoopita.forumfree.it';

// ─── Forum sections — must stay in sync with scraper.py SECTIONS ─────────────
const SECTIONS = [
  ['Bacheca messaggi',                'https://youtubepoopita.forumfree.it/?f=9997591'],
  ['Eventi',                          'https://youtubepoopita.forumfree.it/?f=10249277'],
  ['Restyling',                       'https://youtubepoopita.forumfree.it/?f=9997592'],
  ['Risorse',                         'https://youtubepoopita.forumfree.it/?f=6350394'],
  ['Old sources',                     'https://youtubepoopita.forumfree.it/?f=9965080'],
  ['Biografie YTP',                   'https://youtubepoopita.forumfree.it/?f=6970084'],
  ['Ganons pub',                      'https://youtubepoopita.forumfree.it/?f=6844333'],
  ['YTP fai da te',                   'https://youtubepoopita.forumfree.it/?f=6342067'],
  ['Serve aiuto',                     'https://youtubepoopita.forumfree.it/?f=6350346'],
  ['Il significato della cacca',      'https://youtubepoopita.forumfree.it/?f=9999652'],
  ['Tutorial per il pooping',         'https://youtubepoopita.forumfree.it/?f=10003245'],
  ['Poop in progress',                'https://youtubepoopita.forumfree.it/?f=7071597'],
  ['YTP da internet',                 'https://youtubepoopita.forumfree.it/?f=6350374'],
  ['YTP nostrane',                    'https://youtubepoopita.forumfree.it/?f=10149353'],
  ['YTPMV dimportazione',             'https://youtubepoopita.forumfree.it/?f=6416911'],
  ['Collab poopeschi',                'https://youtubepoopita.forumfree.it/?f=10902086'],
  ['Club sportivo della foca grassa', 'https://youtubepoopita.forumfree.it/?f=6844357'],
  ['Internet memes video',            'https://youtubepoopita.forumfree.it/?f=6342829'],
  ['Altri video',                     'https://youtubepoopita.forumfree.it/?f=6448874'],
  ['Off topic',                       'https://youtubepoopita.forumfree.it/?f=6342068'],
  ['Videogames',                      'https://youtubepoopita.forumfree.it/?f=6350347'],
  ['Cinema',                          'https://youtubepoopita.forumfree.it/?f=6414467'],
  ['Sport',                           'https://youtubepoopita.forumfree.it/?f=10304552'],
  ['Musica',                          'https://youtubepoopita.forumfree.it/?f=6574555'],
  ['Arte e grafica',                  'https://youtubepoopita.forumfree.it/?f=6693231'],
  ['Flood fun',                       'https://youtubepoopita.forumfree.it/?f=10037696'],
  ['THE PIT',                         'https://youtubepoopita.forumfree.it/?f=6342069'],
];

// ─── safe_filename — must produce identical output to scraper.py ──────────────
function safeFilename(name, maxLen = 80) {
  name = name.replace(/[<>:"/\\|?*]/g, '_');
  name = name.replace(/\s+/g, ' ').trim();
  if (name.length > maxLen) name = name.slice(0, maxLen);
  name = name.replace(/[. ]+$/, '');
  return name || '_';
}

// ─── Lookup tables (rebuilt after each scraping run) ─────────────────────────
let forumIdToSection = {};   // "6350394"  → "Risorse"
let sectionToIdx     = {};   // "Risorse"  → 3  (index into SECTIONS array)
let threadToSection  = {};   // "64123456" → { sectionName, title }

function buildLookups() {
  forumIdToSection = {};
  sectionToIdx     = {};
  threadToSection  = {};

  SECTIONS.forEach(([name, surl], i) => {
    const fid = new URL(surl).searchParams.get('f');
    forumIdToSection[fid] = name;
    sectionToIdx[name]    = i;
  });

  let state = {};
  try { state = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8')); } catch {}

  for (const [secName, ss] of Object.entries(state)) {
    const found = Array.isArray(ss.threads_found) ? ss.threads_found : [];
    for (let i = 0; i < found.length; i += 2) {
      const turl  = found[i] || '';
      const title = found[i + 1] || '';
      const m = turl.match(/[?&]t=(\d+)/);
      if (m) threadToSection[m[1]] = { sectionName: secName, title };
    }
  }
}

buildLookups();

// ─── File-system resolution ───────────────────────────────────────────────────

/** Locate a section index page on disk. Returns absolute path or null. */
function findIndexFile(sectionName, pageNum) {
  const safe = safeFilename(sectionName);
  const dir  = path.join(SITE_MIRROR, safe, 'index');
  const base = pageNum === 1
    ? `${safe}.html`
    : `${safe} - pagina ${pageNum}.html`;
  const full = path.join(dir, base);
  return fs.existsSync(full) ? full : null;
}

/**
 * Locate a thread page on disk.
 * Handles both storage layouts:
 *   {section}/{tid}_{title}.html              — single-page thread
 *   {section}/{tid}_{title}/page_{N}.html     — multi-page thread
 */
function findThreadFile(sectionName, threadId, pageNum) {
  const secDir = path.join(SITE_MIRROR, safeFilename(sectionName));
  let entries;
  try { entries = fs.readdirSync(secDir); } catch { return null; }

  const prefix = `${threadId}_`;

  for (const entry of entries) {
    if (!entry.startsWith(prefix)) continue;
    const fullEntry = path.join(secDir, entry);
    const stat = fs.statSync(fullEntry);

    if (stat.isFile() && entry.endsWith('.html') && pageNum === 1) {
      return fullEntry;
    }
    if (stat.isDirectory()) {
      const p = path.join(fullEntry, `page_${pageNum}.html`);
      if (fs.existsSync(p)) return p;
    }
  }
  return null;
}

// ─── HTML rewriting ───────────────────────────────────────────────────────────

const DOMAIN_RE      = new RegExp(`https?://${BASE_DOMAIN.replace(/\./g, '\\.')}/`, 'g');
const DOMAIN_PROT_RE = new RegExp(`//${BASE_DOMAIN.replace(/\./g, '\\.')}/`, 'g');

// Injected before </body>: re-implements the forum's page_jump() for local nav.
const PAGE_JUMP_JS = `
<script>
/* mirror: override page_jump for local navigation */
(function () {
  function page_jump(baseUrl, totalPages, perPage) {
    var p = parseInt(window.prompt('Vai alla pagina (1–' + totalPages + '):', '1'), 10);
    if (p >= 1 && p <= totalPages) {
      var st  = (p - 1) * perPage;
      var sep = baseUrl.indexOf('?') !== -1 ? '&' : '?';
      window.location.href = baseUrl + sep + 'st=' + st;
    }
  }
  window.page_jump = page_jump;
})();
</script>`;

function rewriteHtml(html) {
  // Strip the absolute domain from every link/action/src that references it.
  // Images are already base64-embedded so this only affects navigation URLs.
  html = html.replace(DOMAIN_RE, '/');
  html = html.replace(DOMAIN_PROT_RE, '/');

  // Inject page_jump override at the very end of the document so it wins
  // over any earlier definition coming from the original forum scripts.
  if (html.includes('</body>')) {
    html = html.replace('</body>', PAGE_JUMP_JS + '\n</body>');
  } else {
    html += PAGE_JUMP_JS;
  }
  return html;
}

// ─── On-demand scraping ───────────────────────────────────────────────────────

const activeJobs = new Map();  // jobKey → spawned process

/**
 * Make sure threadUrl appears in the section's threads_found list so that
 * a subsequent `scraper.py --sections N` run will pick it up.
 */
function ensureThreadInState(threadId, threadUrl, sectionName) {
  let state = {};
  try { state = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8')); } catch {}

  const ss = state[sectionName];
  if (!ss) return;

  const known = new Set((ss.threads_found || []).filter((_, i) => i % 2 === 0));
  if (!known.has(threadUrl)) {
    ss.threads_found.push(threadUrl, `Thread ${threadId}`);
    fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
    buildLookups();
  }
}

/**
 * Spawn scraper.py for a single thread URL (fast path) or a whole section.
 * jobKey prevents duplicate concurrent runs for the same resource.
 */
function triggerScrape(jobKey, sectionIdx, threadUrl) {
  if (activeJobs.has(jobKey)) return;

  const args = ['scraper.py', '--sections', String(sectionIdx)];
  if (threadUrl) args.push('--thread-url', threadUrl);

  console.log(`[scraper] start  key="${jobKey}"  cmd: python ${args.join(' ')}`);

  const proc = spawn('python', args, {
    cwd: __dirname,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  activeJobs.set(jobKey, proc);

  proc.stdout.on('data', d => process.stdout.write('[scraper] ' + d));
  proc.stderr.on('data', d => process.stderr.write('[scraper] ' + d));
  proc.on('close', code => {
    activeJobs.delete(jobKey);
    buildLookups();
    console.log(`[scraper] finish key="${jobKey}" (exit ${code})`);
  });
}

// ─── HTTP response helpers ────────────────────────────────────────────────────

function sendHtml(res, code, body) {
  res.writeHead(code, { 'Content-Type': 'text/html; charset=utf-8' });
  res.end(body);
}

function serveFile(filePath, res) {
  const raw = fs.readFileSync(filePath, 'utf8');
  sendHtml(res, 200, rewriteHtml(raw));
}

function serveWaiting(res, message) {
  sendHtml(res, 202, `<!DOCTYPE html>
<html lang="it">
<head>
  <meta charset="utf-8">
  <meta http-equiv="refresh" content="10">
  <title>Scraping in corso…</title>
  <style>
    * { box-sizing: border-box; }
    body { font-family: sans-serif; background: #111; color: #eee; margin: 0;
           display: flex; flex-direction: column; align-items: center;
           justify-content: center; min-height: 100vh; text-align: center; }
    h2   { margin-bottom: .4em; }
    p    { color: #aaa; margin: .3em 0; }
    .ico { font-size: 3rem; animation: spin 1.5s linear infinite; display: inline-block; }
    @keyframes spin { to { transform: rotate(360deg); } }
    a    { color: #6af; }
  </style>
</head>
<body>
  <div class="ico">⏳</div>
  <h2>Scraping in corso…</h2>
  <p>${message}</p>
  <p style="font-size:.85em">Questa pagina si aggiorna automaticamente ogni 10 secondi.</p>
  <p><a href="/">← Home</a></p>
</body>
</html>`);
}

function serveNotFound(res, detail) {
  sendHtml(res, 404, `<!DOCTYPE html>
<html lang="it">
<head>
  <meta charset="utf-8"><title>Non trovato</title>
  <style>
    body { font-family: sans-serif; max-width: 640px; margin: 4rem auto; padding: 0 1rem; }
    a    { color: #06c; }
  </style>
</head>
<body>
  <h2>Pagina non trovata</h2>
  <p>${detail}</p>
  <p><a href="/">← Torna alla Home</a></p>
</body>
</html>`);
}

// ─── Request handler ──────────────────────────────────────────────────────────

function onRequest(req, res) {
  if (req.method !== 'GET' && req.method !== 'HEAD') {
    res.writeHead(405); res.end(); return;
  }

  let reqUrl;
  try {
    reqUrl = new URL(req.url, `http://localhost:${PORT}`);
  } catch {
    res.writeHead(400); res.end(); return;
  }

  const fid     = reqUrl.searchParams.get('f');
  const tid     = reqUrl.searchParams.get('t');
  const st      = parseInt(reqUrl.searchParams.get('st') || '0', 10);
  // Both section indices and thread pages use st increments of 30 (scraper convention)
  const pageNum = st > 0 ? Math.floor(st / 30) + 1 : 1;

  // ── Home ─────────────────────────────────────────────────────────────────
  if (!fid && !tid) {
    const p = path.join(SITE_MIRROR, 'Home.html');
    if (fs.existsSync(p)) return serveFile(p, res);
    return serveNotFound(res, 'Home.html non trovato nella mirror.');
  }

  // ── Section index ─────────────────────────────────────────────────────────
  if (fid && !tid) {
    const sectionName = forumIdToSection[fid];
    if (!sectionName) {
      return serveNotFound(res, `Sezione f=${fid} non riconosciuta.`);
    }

    const filePath = findIndexFile(sectionName, pageNum);
    if (filePath) return serveFile(filePath, res);

    // Not yet scraped — trigger and wait
    triggerScrape(`f:${fid}:p${pageNum}`, sectionToIdx[sectionName], null);
    return serveWaiting(res,
      `Download pagina ${pageNum} dell&rsquo;indice &ldquo;${sectionName}&rdquo;&hellip;`);
  }

  // ── Thread page ───────────────────────────────────────────────────────────
  if (tid) {
    const info = threadToSection[tid];

    if (!info) {
      return serveNotFound(res,
        `Thread t=${tid} non presente nella mirror. ` +
        `<a href="https://${BASE_DOMAIN}/?t=${tid}" target="_blank" rel="noopener">` +
        `Apri sul forum ↗</a>`);
    }

    const { sectionName, title } = info;
    const filePath = findThreadFile(sectionName, tid, pageNum);
    if (filePath) return serveFile(filePath, res);

    // Known but not downloaded — ensure it's in state, then scrape
    const threadUrl = `https://${BASE_DOMAIN}/?t=${tid}`;
    ensureThreadInState(tid, threadUrl, sectionName);
    triggerScrape(`t:${tid}`, sectionToIdx[sectionName], threadUrl);

    return serveWaiting(res,
      `Scraping thread &ldquo;${title || 'Thread ' + tid}&rdquo; (pagina ${pageNum})&hellip;`);
  }
}

// ─── Start server ─────────────────────────────────────────────────────────────

const server = http.createServer(onRequest);

server.listen(PORT, () => {
  let state = {};
  try { state = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8')); } catch {}

  const threadCount = Object.values(state).reduce(
    (n, s) => n + Math.floor(((s.threads_found || []).length) / 2), 0
  );
  const doneCount = Object.values(state).reduce(
    (n, s) => n + (s.threads_done || []).length, 0
  );

  console.log(`\nYTP Forum Mirror — server avviato`);
  console.log(`  URL:      http://localhost:${PORT}`);
  console.log(`  Mirror:   ${SITE_MIRROR}`);
  console.log(`  Sezioni:  ${SECTIONS.length}`);
  console.log(`  Thread:   ${doneCount} scaricati / ${threadCount} in stato`);
  console.log();
});
