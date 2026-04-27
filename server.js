#!/usr/bin/env node
/**
 * YTP Archive Dashboard Server
 *
 * Serves the main archive dashboard (docs/index.html) and local video files.
 */

'use strict';

if (process.argv.includes('forum')) {
  require('./server_forum.js');
  return;
}

const http  = require('http');
const fs    = require('fs');
const path  = require('path');
const { URL } = require('url');

const PORT        = parseInt(process.env.PORT || '3000', 10);
const DOCS_DIR    = path.join(__dirname, 'docs');
const VIDEOS_DIR  = path.join(__dirname, 'videos');
const VIDEO_INDEX = path.join(DOCS_DIR, 'video_index.json');
const EXCLUDED_VIDEOS = path.join(DOCS_DIR, 'excluded_videos.json');
const SOURCES_INDEX = path.join(DOCS_DIR, 'sources_index.json');

const VM_LOGIC    = require('./video_manager.js');

const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.js':   'application/javascript',
  '.css':  'text/css',
  '.json': 'application/json',
  '.png':  'image/png',
  '.jpg':  'image/jpeg',
  '.webp': 'image/webp',
  '.webm': 'video/webm',
  '.mp4':  'video/mp4',
  '.ico':  'image/x-icon',
};

// ─── Local video file serving (with range-request support for seeking) ────────

function serveLocalVideo(filePath, req, res) {
  let stat;
  try { stat = fs.statSync(filePath); } catch {
    res.writeHead(404); res.end('Not found'); return;
  }

  const total = stat.size;
  const range = req.headers['range'];

  if (range) {
    const [, s, e] = range.replace(/bytes=/, '').match(/^(\d*)-(\d*)$/) || [];
    const start = s ? parseInt(s, 10) : 0;
    const end   = e ? parseInt(e, 10) : total - 1;
    res.writeHead(206, {
      'Content-Range':  `bytes ${start}-${end}/${total}`,
      'Accept-Ranges':  'bytes',
      'Content-Length': end - start + 1,
      'Content-Type':   'video/mp4',
    });
    fs.createReadStream(filePath, { start, end }).pipe(res);
  } else {
    res.writeHead(200, {
      'Content-Length': total,
      'Content-Type':   'video/mp4',
      'Accept-Ranges':  'bytes',
    });
    fs.createReadStream(filePath).pipe(res);
  }
}

// ─── Request handler ──────────────────────────────────────────────────────────

function onRequest(req, res) {
  if (req.method !== 'GET' && req.method !== 'HEAD' && req.method !== 'POST') {
    res.writeHead(405); res.end(); return;
  }

  let reqUrl;
  try {
    reqUrl = new URL(req.url, `http://localhost:${PORT}`);
  } catch {
    res.writeHead(400); res.end(); return;
  }

  const pathname = reqUrl.pathname;

  // ── API: Ban Videos ──────────────────────────────────────────────────────
  if (pathname === '/api/ban' && req.method === 'POST') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
      try {
        const { videoIds } = JSON.parse(body);
        const result = VM_LOGIC.banVideos(videoIds, VIDEO_INDEX, EXCLUDED_VIDEOS, VIDEOS_DIR);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(result));
      } catch (err) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: false, error: err.message }));
      }
    });
    return;
  }

  // ── API: Flag as Source ──────────────────────────────────────────────────
  if (pathname === '/api/flag-source' && req.method === 'POST') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
      try {
        const { videoIds } = JSON.parse(body);
        const result = VM_LOGIC.flagAsSource(videoIds, VIDEO_INDEX, SOURCES_INDEX);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(result));
      } catch (err) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: false, error: err.message }));
      }
    });
    return;
  }

  // ── Local video files ─────────────────────────────────────────────────────
  if (pathname.startsWith('/local/')) {
    const rel = pathname.slice('/local/'.length)
      .split('/').map(decodeURIComponent).join(path.sep);
    const filePath = path.join(VIDEOS_DIR, rel);
    if (!filePath.startsWith(VIDEOS_DIR + path.sep) && filePath !== VIDEOS_DIR) {
      res.writeHead(403); res.end(); return;
    }
    return serveLocalVideo(filePath, req, res);
  }

  // ── Static files from docs ────────────────────────────────────────────────
  let relPath = pathname === '/' ? 'index.html' : pathname.replace(/^\//, '');
  let filePath = path.join(DOCS_DIR, relPath);

  // Fallback for video_manager path
  if (pathname === '/video_manager') {
    filePath = path.join(__dirname, 'video_manager', 'video_manager.html');
  } else if (pathname === '/video_manager_client.js') {
    filePath = path.join(__dirname, 'video_manager', 'video_manager_client.js');
  }

  if (fs.existsSync(filePath) && fs.statSync(filePath).isFile()) {
    const ext = path.extname(filePath).toLowerCase();
    res.writeHead(200, { 'Content-Type': MIME_TYPES[ext] || 'application/octet-stream' });
    return fs.createReadStream(filePath).pipe(res);
  }

  res.writeHead(404);
  res.end('Not Found');
}

// ─── Start server ─────────────────────────────────────────────────────────────

const server = http.createServer(onRequest);

server.listen(PORT, () => {
  console.log(`\nYTP Archive Dashboard — server avviato`);
  console.log(`  URL:      http://localhost:${PORT}`);
  console.log(`  Docs:     ${DOCS_DIR}`);
  console.log();
});
