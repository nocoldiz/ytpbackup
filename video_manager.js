'use strict';

const fs = require('fs');
const path = require('path');

/**
 * Bans a list of videos:
 * 1. Removes local files.
 * 2. Adds to excluded_videos.json.
 * 3. Removes from video_index.json.
 * 
 * @param {string[]} videoIds 
 * @param {string} videoIndexPath 
 * @param {string} excludedVideosPath 
 * @param {string} videosDir 
 */
function banVideos(videoIds, videoIndexPath, excludedVideosPath, videosDir) {
  let index = {};
  try {
    index = JSON.parse(fs.readFileSync(videoIndexPath, 'utf8'));
  } catch (err) {
    console.error(`[video_manager] Error reading video index: ${err.message}`);
    return { success: false, error: 'Failed to read video index' };
  }

  let excluded = {};
  try {
    if (fs.existsSync(excludedVideosPath)) {
      excluded = JSON.parse(fs.readFileSync(excludedVideosPath, 'utf8'));
    }
  } catch (err) {
    console.error(`[video_manager] Error reading excluded videos: ${err.message}`);
  }

  const results = {
    deleted: [],
    failed: [],
    skipped: []
  };

  for (const id of videoIds) {
    if (!index[id]) {
      results.skipped.push(id);
      continue;
    }

    const entry = index[id];

    // 1. Remove local file
    if (entry.local_file) {
      const filePath = path.join(__dirname, entry.local_file);
      try {
        if (fs.existsSync(filePath)) {
          fs.unlinkSync(filePath);
          console.log(`[video_manager] Deleted local file: ${filePath}`);
        }
      } catch (err) {
        console.error(`[video_manager] Failed to delete file ${filePath}: ${err.message}`);
      }
    }

    // 2. Add to excluded
    excluded[id] = entry;

    // 3. Remove from index
    delete index[id];

    results.deleted.push(id);
  }

  // Save changes
  try {
    fs.writeFileSync(videoIndexPath, JSON.stringify(index, null, 2));
    fs.writeFileSync(excludedVideosPath, JSON.stringify(excluded, null, 2));
    return { success: true, results };
  } catch (err) {
    console.error(`[video_manager] Error saving JSON files: ${err.message}`);
    return { success: false, error: 'Failed to save changes' };
  }
}

module.exports = {
  banVideos
};

// ─── CLI / Standalone Server Handler ─────────────────────────────────────────
if (require.main === module) {
  const ids = process.argv.slice(2);
  const vIndex = path.join(__dirname, 'docs', 'video_index.json');
  const vExcl = path.join(__dirname, 'excluded_videos.json');
  const vDir = path.join(__dirname, 'videos');

  if (ids.length > 0) {
    // CLI Mode: Ban specific IDs
    console.log(`[cli] Banning ${ids.length} videos...`);
    const res = banVideos(ids, vIndex, vExcl, vDir);
    if (res.success) {
      console.log(`[cli] Success! Deleted: ${res.results.deleted.length}, Skipped: ${res.results.skipped.length}`);
    } else {
      console.error(`[cli] Failed: ${res.error}`);
      process.exit(1);
    }
  } else {
    // Server Mode: Start a minimal manager server
    const http = require('http');
    const PORT = 3001;

    const server = http.createServer((req, res) => {
      const url = new URL(req.url, `http://localhost:${PORT}`);

      // Serve Manager HTML
      if (url.pathname === '/' || url.pathname === '/video_manager') {
        const p = path.join(__dirname, 'video_manager', 'video_manager.html');
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        return res.end(fs.readFileSync(p));
      }

      // Serve Manager Client JS
      if (url.pathname === '/video_manager_client.js') {
        const p = path.join(__dirname, 'video_manager', 'video_manager_client.js');
        res.writeHead(200, { 'Content-Type': 'application/javascript' });
        return res.end(fs.readFileSync(p));
      }

      // Serve Docs (for video_index.json and CSS)
      if (url.pathname.startsWith('/docs/')) {
        const rel = url.pathname.slice('/docs/'.length).split('/').map(decodeURIComponent).join(path.sep);
        const p = path.join(__dirname, 'docs', rel);
        if (fs.existsSync(p) && fs.statSync(p).isFile()) {
          const ext = path.extname(p).toLowerCase();
          const mime = { '.css': 'text/css', '.json': 'application/json', '.js': 'application/javascript' }[ext] || 'application/octet-stream';
          res.writeHead(200, { 'Content-Type': mime });
          return fs.createReadStream(p).pipe(res);
        }
      }

      // Ban API
      if (url.pathname === '/api/ban' && req.method === 'POST') {
        let body = '';
        req.on('data', chunk => { body += chunk; });
        req.on('end', () => {
          try {
            const { videoIds } = JSON.parse(body);
            const result = banVideos(videoIds, vIndex, vExcl, vDir);
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify(result));
          } catch (err) {
            res.writeHead(400).end(JSON.stringify({ success: false, error: err.message }));
          }
        });
        return;
      }

      res.writeHead(404).end('Not Found');
    });

    server.listen(PORT, () => {
      console.log(`\nVideo Manager Standalone Server running at:`);
      console.log(`  http://localhost:${PORT}`);
      console.log(`  (Press Ctrl+C to stop)\n`);
    });
  }
}
