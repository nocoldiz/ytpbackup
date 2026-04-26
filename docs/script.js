// ─── STATE ───────────────────────────────────────────────────────────────
let allVideos = [];       // [{id, ...fields}] - filtered to ALLOWED_SECTIONS
let rawVideos = [];       // all videos unfiltered, used by Sources tab
let filteredVideos = [];
let currentPage = 1;
const PAGE_SIZE = 50;
let selectedChannel = null;
let selectedSection = null;
let charts = {};

// ─── LOADING ─────────────────────────────────────────────────────────────
document.getElementById('fileInput').addEventListener('change', e => {
  const f = e.target.files[0];
  if (f) loadFile(f);
});

const dz = document.getElementById('dropZone');
dz.addEventListener('dragover', e => { e.preventDefault(); dz.classList.add('drag-over'); });
dz.addEventListener('dragleave', () => dz.classList.remove('drag-over'));
dz.addEventListener('drop', e => {
  e.preventDefault(); dz.classList.remove('drag-over');
  const f = e.dataTransfer.files[0];
  if (f) loadFile(f);
});

function loadFile(file) {
  const reader = new FileReader();
  reader.onload = e => initApp(JSON.parse(e.target.result));
  reader.readAsText(file);
}

// Auto-load from same directory
fetch('video_index.json')
  .then(r => r.json())
  .then(data => initApp(data))
  .catch(() => { }); // silently fail if not found

// ─── INIT ─────────────────────────────────────────────────────────────────
const ALLOWED_SECTIONS = new Set(["YTP nostrane", "YTP fai da te", "YTPMV dimportazione", "YTP da internet", "Internet", "Youtube", "Scraped Channel"]);
const SOURCES_SECTIONS = new Set(["Risorse", "Tutorial per il pooping", "Old Sources"]);

function initApp(raw) {
  rawVideos = Object.entries(raw).map(([id, v]) => ({ id, ...v }));
  allVideos = rawVideos.filter(v => (v.sections || []).some(s => ALLOWED_SECTIONS.has(s)));
  document.getElementById('landing').style.display = 'none';
  document.getElementById('app').style.display = 'block';

  // badges
  const channels = new Set(allVideos.map(v => v.channel_name).filter(Boolean));
  const sections = new Set(allVideos.flatMap(v => v.sections || []));
  document.getElementById('badge-videos').textContent = allVideos.length;
  document.getElementById('badge-channels').textContent = channels.size;
  document.getElementById('badge-sections').textContent = sections.size;

  const years = new Set(allVideos.map(v => v.publish_date ? v.publish_date.slice(0, 4) : null).filter(Boolean));
  document.getElementById('badge-years').textContent = years.size;

  buildFilterOptions();
  buildOverview();
  applyFilters();
  renderChannelGrid();
  renderSectionGrid();
  renderYearGrid();
  const srcData = buildSourcesData();
  document.getElementById('badge-sources').textContent = srcData.length;
  initSourcesFilters(srcData);
  renderSourcesTable();
}

// ─── NAVIGATION ──────────────────────────────────────────────────────────
function showPage(name) {
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-tab').forEach(t => t.classList.remove('active'));
  document.getElementById('page-' + name).classList.add('active');
  event.target.classList.add('active');
}

// ─── FILTER OPTIONS ───────────────────────────────────────────────────────
function buildFilterOptions() {
  const sectionSel = document.getElementById('filter-section');
  const channelSel = document.getElementById('filter-channel');
  const yearSel = document.getElementById('filter-year');

  const sections = [...new Set(allVideos.flatMap(v => v.sections || []))].sort();
  sections.forEach(s => {
    const o = document.createElement('option'); o.value = s; o.textContent = s;
    sectionSel.appendChild(o);
  });

  const channels = [...new Set(allVideos.map(v => v.channel_name).filter(Boolean))].sort();
  channels.forEach(c => {
    const o = document.createElement('option'); o.value = c; o.textContent = c;
    channelSel.appendChild(o);
  });

  const years = [...new Set(allVideos.map(v => v.publish_date ? v.publish_date.slice(0, 4) : null).filter(Boolean))].sort();
  years.forEach(y => {
    const o = document.createElement('option'); o.value = y; o.textContent = y;
    yearSel.appendChild(o);
  });
}

// ─── FILTERS + TABLE ──────────────────────────────────────────────────────
let sortField = 'publish_date';
let sortDir = 1;
let scrollObserver = null;

function applyFilters() {
  const q = document.getElementById('search-input').value.toLowerCase().trim();
  const status = document.getElementById('filter-status').value;
  const section = document.getElementById('filter-section').value;
  const channel = document.getElementById('filter-channel').value;
  const viewsMin = parseInt(document.getElementById('filter-views-min').value) || 0;
  const likesMin = parseInt(document.getElementById('filter-likes-min').value) || 0;
  const year = document.getElementById('filter-year').value;

  filteredVideos = allVideos.filter(v => {
    // 1. IMPROVED SEARCH BAR LOGIC
    if (q) {
      // Gather all searchable fields into one lowercase string
      const haystack = [
        v.title,
        v.channel_name,
        v.description,
        ...(v.thread_titles || []),
        ...(v.tags || [])
      ].join(' ').toLowerCase();

      // Split the query into individual words (ignoring extra spaces)
      const searchTerms = q.split(/\s+/);

      // Ensure EVERY typed word is found somewhere in the video's data
      const matchesAllTerms = searchTerms.every(term => haystack.includes(term));

      if (!matchesAllTerms) return false;
    }

    // 2. Exact Match Filters
    if (status && v.status !== status) return false;
    if (section && !(v.sections || []).includes(section)) return false;
    if (channel && v.channel_name !== channel) return false;
    if (viewsMin && (v.view_count || 0) < viewsMin) return false;
    if (likesMin && (v.like_count || 0) < likesMin) return false;
    if (year && (!v.publish_date || !v.publish_date.startsWith(year))) return false;

    return true;
  });

  // 3. Sorting Logic
  filteredVideos.sort((a, b) => {
    let av = a[sortField] || '';
    let bv = b[sortField] || '';
    if (typeof av === 'string') { av = av.toLowerCase(); bv = bv.toLowerCase(); }
    if (av > bv) return sortDir;
    if (av < bv) return -sortDir;
    return 0;
  });

  currentPage = 1;
  renderTable(false);
  setupScrollObserver();
}

function sortTable(field) {
  if (sortField === field) sortDir *= -1;
  else { sortField = field; sortDir = 1; }

  // Update header UI
  document.querySelectorAll('#video-table th').forEach(th => {
    th.classList.remove('sorted');
    const arrow = th.querySelector('.sort-arrow');
    if (arrow) arrow.textContent = '↕';
  });
  const th = document.querySelector(`#video-table th[data-field="${field}"]`);
  if (th) {
    th.classList.add('sorted');
    const arrow = th.querySelector('.sort-arrow');
    if (arrow) arrow.textContent = sortDir === 1 ? '↓' : '↑';
  }

  filteredVideos.sort((a, b) => {
    let av = a[sortField] || '';
    let bv = b[sortField] || '';
    if (typeof av === 'string') { av = av.toLowerCase(); bv = bv.toLowerCase(); }
    if (av > bv) return sortDir;
    if (av < bv) return -sortDir;
    return 0;
  });

  currentPage = 1;
  renderTable(false);
}

function setupScrollObserver() {
  if (scrollObserver) scrollObserver.disconnect();
  const sentinel = document.getElementById('scroll-sentinel');
  if (!sentinel) return;

  scrollObserver = new IntersectionObserver((entries) => {
    if (entries[0].isIntersecting) {
      if (currentPage * PAGE_SIZE < filteredVideos.length) {
        currentPage++;
        renderTable(true);
      }
    }
  }, { rootMargin: '200px' });

  scrollObserver.observe(sentinel);
}

function renderTable(append = false) {
  const tbody = document.getElementById('video-tbody');
  const total = filteredVideos.length;
  document.getElementById('videos-count-label').textContent = `${total} videos`;

  const start = (currentPage - 1) * PAGE_SIZE;
  const slice = filteredVideos.slice(start, start + PAGE_SIZE);

  const html = slice.map(v => {
    const statusClass = 'status-' + (v.status || 'unavailable');

    const threads = (v.source_pages || [])
      // 1. Check if the string includes 'channel_scrape' and exclude it if it does
      .filter(sp => !sp.includes('channel_scrape'))
      // 2. Map over whatever is left to create your links
      .map((sp, i) => {
        const path = 'https://github.com/nocoldiz/ytpbackup/blob/main/site_mirror/' + sp.replace(/\\/g, '/');


        const label = (v.thread_titles || [])[i] || sp;


        return `<a class="btn-thread" href="${path}" target="_blank" title="${escHtml(label)}">📄 ${escHtml(label.length > 22 ? label.slice(0, 22) + '…' : label)}</a>`;
      }).join(' ');
    const sections = (v.sections || []).map(s => `<span class="tag-pill">${escHtml(s)}</span>`).join('');

    // Determine the title to display, falling back to the first thread title if v.title is missing
    const fallbackTitle = (v.thread_titles && v.thread_titles[0]) ? v.thread_titles[0] : null;
    const titleContent = v.title
      ? `<a href="${v.url}" target="_blank">${escHtml(v.title)}</a>`
      : (fallbackTitle ? `<a href="${v.url}" target="_blank"><em>${escHtml(fallbackTitle)}</em></a>` : `<span class="vid-id">${v.id}</span>`);

    return `<tr>
          <td class="title-cell">
            ${titleContent}
            <div class="vid-id">${v.id}</div>
          </td>
          <td>${v.channel_name ? `<a href="${v.channel_url || '#'}" target="_blank" style="color:var(--text-muted);text-decoration:none">${escHtml(v.channel_name)}</a>` : '-'}</td>
          <td>${v.publish_date ? v.publish_date.slice(0, 10) : '-'}</td>
          <td><span class="status-dot status-${statusClass}"></span><span class="status-text">${v.status || '-'}</span></td>
          <td class="num">${fmtNum(v.view_count)}</td>
          <td class="num">${fmtNum(v.like_count)}</td>
          <td>${sections || '-'}</td>
          <td>${threads || '-'}</td>
          <td><a class="btn-yt" href="${v.url}" target="_blank">YT</a></td>
        </tr>`;
  }).join('') || (append ? '' : `<tr><td colspan="8" class="empty">No videos match your filters</td></tr>`);

  if (append) {
    tbody.insertAdjacentHTML('beforeend', html);
  } else {
    tbody.innerHTML = html;
  }

  renderPagination(total);
}

function renderPagination(total) {
  const el = document.getElementById('pagination');
  if (total === 0) { el.innerHTML = ''; return; }
  const showing = Math.min(currentPage * PAGE_SIZE, total);
  el.innerHTML = `<span class="page-info">Showing ${showing} of ${total} results (Page ${currentPage})</span>`;
}

// ─── CHANNELS ─────────────────────────────────────────────────────────────
function buildChannelData() {
  const map = {};
  allVideos.forEach(v => {
    const ch = v.channel_name;
    if (!ch) return;
    if (!map[ch]) map[ch] = { name: ch, url: v.channel_url, videos: [], totalViews: 0, totalLikes: 0 };
    map[ch].videos.push(v);
    map[ch].totalViews += v.view_count || 0;
    map[ch].totalLikes += v.like_count || 0;
  });
  return Object.values(map).sort((a, b) => b.videos.length - a.videos.length);
}

function renderChannelGrid() {
  const q = (document.getElementById('channel-search').value || '').toLowerCase();
  const channels = buildChannelData().filter(c => !q || c.name.toLowerCase().includes(q));
  document.getElementById('channels-count-label').textContent = `${channels.length} channels`;
  document.getElementById('channel-grid').innerHTML = channels.map(c => `
    <div class="channel-card${selectedChannel === c.name ? ' selected' : ''}" onclick="selectChannel('${escAttr(c.name)}')">
      <h4>${escHtml(c.name)}</h4>
      <div class="ch-stats">
        <span><strong>${c.videos.length}</strong> videos</span>
        ${c.totalViews ? `<span><strong>${fmtNum(c.totalViews)}</strong> views</span>` : ''}
        ${c.totalLikes ? `<span><strong>${fmtNum(c.totalLikes)}</strong> likes</span>` : ''}
      </div>
    </div>`).join('') || `<div class="empty">No channels found</div>`;
}

function selectChannel(name) {
  selectedChannel = selectedChannel === name ? null : name;
  renderChannelGrid();
  const panel = document.getElementById('channel-detail-panel');
  if (!selectedChannel) { panel.style.display = 'none'; return; }

  // Scroll to top of page
  window.scrollTo({ top: 0, behavior: 'smooth' });

  const channels = buildChannelData();
  const ch = channels.find(c => c.name === name);
  if (!ch) return;

  // Videos by year
  const byYear = {};
  ch.videos.forEach(v => {
    const y = v.publish_date ? v.publish_date.slice(0, 4) : 'Unknown';
    byYear[y] = (byYear[y] || 0) + 1;
  });
  const years = Object.keys(byYear).sort();

  const statusCount = {};
  ch.videos.forEach(v => { statusCount[v.status || 'unknown'] = (statusCount[v.status || 'unknown'] || 0) + 1; });

  // Sort videos by date (oldest first)
  const sortedVideos = [...ch.videos].sort((a, b) => {
    const da = a.publish_date || '';
    const db = b.publish_date || '';
    return da.localeCompare(db);
  });

  // Build video list HTML
  const videoListHtml = sortedVideos.map(v => {
    const statusClass = 'status-' + (v.status || 'unavailable');
    return `<tr>
      <td class="title-cell"><a href="${v.url}" target="_blank" style="color:var(--text);text-decoration:none">${escHtml(v.title || v.id)}</a></td>
      <td>${v.publish_date ? v.publish_date.slice(0, 10) : '-'}</td>
      <td><span class="status-dot ${statusClass}"></span><span class="status-text">${v.status || '-'}</span></td>
      <td class="num">${fmtNum(v.view_count)}</td>
      <td class="num">${fmtNum(v.like_count)}</td>
      <td><a class="btn-yt" href="${v.url}" target="_blank">YT</a></td>
    </tr>`;
  }).join('');

  panel.style.display = 'block';
  panel.innerHTML = `<div class="channel-detail">
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:8px">
      <h3>${escHtml(ch.name)}</h3>
      <button class="close-btn" onclick="selectedChannel=null;document.getElementById('channel-detail-panel').style.display='none';renderChannelGrid()">✕ Close</button>
    </div>
    <div class="ch-url">${ch.url ? `<a href="${ch.url}" target="_blank">${escHtml(ch.url)}</a>` : 'No URL'}</div>
    <div class="ch-detail-grid">
      <div class="chart-card">
        <h3>Videos by Year</h3>
        <canvas id="ch-year-chart" style="max-height:220px"></canvas>
      </div>
      <div class="chart-card">
        <h3>Status Breakdown</h3>
        <canvas id="ch-status-chart" style="max-height:220px"></canvas>
      </div>
    </div>
    <div style="margin-top:20px">
      <h3 style="font-size:.9rem;color:var(--text-muted);text-transform:uppercase;letter-spacing:.06em;margin-bottom:12px">All Videos (${ch.videos.length})</h3>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Title</th>
              <th>Date</th>
              <th>Status</th>
              <th>Views</th>
              <th>Likes</th>
              <th></th>
            </tr>
          </thead>
          <tbody>${videoListHtml}</tbody>
        </table>
      </div>
    </div>
  </div>`;

  setTimeout(() => {
    destroyChart('ch-year'); destroyChart('ch-status');
    charts['ch-year'] = new Chart(document.getElementById('ch-year-chart'), {
      type: 'bar',
      data: { labels: years, datasets: [{ label: 'Videos', data: years.map(y => byYear[y]), backgroundColor: PALETTE[0] + 'cc', borderRadius: 6 }] },
      options: chartOpts('Videos per year')
    });
    charts['ch-status'] = new Chart(document.getElementById('ch-status-chart'), {
      type: 'doughnut',
      data: { labels: Object.keys(statusCount), datasets: [{ data: Object.values(statusCount), backgroundColor: [STATUS_COLORS.available, STATUS_COLORS.unavailable, STATUS_COLORS.pending, '#888'] }] },
      options: { ...pieOpts(), plugins: { ...pieOpts().plugins, title: { display: false } } }
    });
  }, 50);
}

// ─── SECTIONS ─────────────────────────────────────────────────────────────
function buildSectionData() {
  const map = {};
  allVideos.forEach(v => {
    (v.sections || []).forEach(s => {
      if (!map[s]) map[s] = { name: s, videos: [], totalViews: 0 };
      map[s].videos.push(v);
      map[s].totalViews += v.view_count || 0;
    });
  });
  return Object.values(map).sort((a, b) => b.videos.length - a.videos.length);
}

function renderSectionGrid() {
  const sections = buildSectionData();
  document.getElementById('section-grid').innerHTML = sections.map(s => `
    <div class="section-card${selectedSection === s.name ? ' selected' : ''}" onclick="selectSection('${escAttr(s.name)}')">
      <h4>${escHtml(s.name)}</h4>
      <div class="s-count">${s.videos.length}</div>
      <div class="s-sub">${fmtNum(s.totalViews)} total views</div>
    </div>`).join('');
}

function selectSection(name) {
  selectedSection = selectedSection === name ? null : name;
  renderSectionGrid();
  const panel = document.getElementById('section-detail-panel');
  if (!selectedSection) { panel.style.display = 'none'; return; }

  const sec = buildSectionData().find(s => s.name === name);
  const byYear = {};
  sec.videos.forEach(v => {
    const y = v.publish_date ? v.publish_date.slice(0, 4) : 'Unknown';
    byYear[y] = (byYear[y] || 0) + 1;
  });
  const years = Object.keys(byYear).sort();

  panel.style.display = 'block';
  panel.innerHTML = `<div class="channel-detail" style="border-color:var(--accent2)">
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:16px">
      <h3>${escHtml(sec.name)}</h3>
      <span style="color:var(--text-muted)">${sec.videos.length} videos · ${fmtNum(sec.totalViews)} views</span>
      <button class="close-btn" onclick="selectedSection=null;document.getElementById('section-detail-panel').style.display='none';renderSectionGrid()">✕ Close</button>
    </div>
    <div class="chart-card" style="max-width:600px">
      <h3>Videos by Year in "${escHtml(sec.name)}"</h3>
      <canvas id="sec-year-chart" style="max-height:200px"></canvas>
    </div>
  </div>`;

  setTimeout(() => {
    destroyChart('sec-year');
    charts['sec-year'] = new Chart(document.getElementById('sec-year-chart'), {
      type: 'bar',
      data: { labels: years, datasets: [{ label: 'Videos', data: years.map(y => byYear[y]), backgroundColor: PALETTE[1] + 'cc', borderRadius: 6 }] },
      options: chartOpts('Videos per year')
    });
  }, 50);
}

// ─── YEARS ────────────────────────────────────────────────────────────────
let selectedYear = null;

function buildYearData() {
  const map = {};
  allVideos.forEach(v => {
    const y = v.publish_date ? v.publish_date.slice(0, 4) : null;
    if (!y) return;
    if (!map[y]) map[y] = { year: y, videos: [], totalViews: 0, totalLikes: 0 };
    map[y].videos.push(v);
    map[y].totalViews += v.view_count || 0;
    map[y].totalLikes += v.like_count || 0;
  });
  return Object.values(map).sort((a, b) => a.year.localeCompare(b.year));
}

function renderYearGrid() {
  const years = buildYearData();
  document.getElementById('years-count-label').textContent = `${years.length} years`;
  const maxCount = Math.max(...years.map(y => y.videos.length));
  document.getElementById('year-grid').innerHTML = years.map(y => `
    <div class="year-card${selectedYear === y.year ? ' selected' : ''}" onclick="selectYear('${y.year}')">
      <div class="y-year">${y.year}</div>
      <div class="y-count">${y.videos.length} video${y.videos.length !== 1 ? 's' : ''}</div>
      <div class="y-bar" style="width:${Math.round(y.videos.length / maxCount * 100)}%"></div>
    </div>`).join('') || `<div class="empty">No dated videos</div>`;
}

function selectYear(year) {
  selectedYear = selectedYear === year ? null : year;
  renderYearGrid();
  const panel = document.getElementById('year-detail-panel');
  if (!selectedYear) { panel.style.display = 'none'; return; }

  const allYears = buildYearData();
  const yd = allYears.find(y => y.year === year);
  if (!yd) return;

  // Most prolific creator
  const crCounts = {};
  yd.videos.forEach(v => { if (v.channel_name) crCounts[v.channel_name] = (crCounts[v.channel_name] || 0) + 1; });
  const topCreators = Object.entries(crCounts).sort((a, b) => b[1] - a[1]);
  const topCreator = topCreators[0];

  // Tag frequency
  const TAG_BLOCKLIST = new Set(["poop", "youtube", "ytp", "ita", "merda", "youtube merda", "ytp ita", "ytpmv", "youtube merda"]);
  const tagCounts = {};
  yd.videos.forEach(v => (v.tags || []).forEach(t => {
    const tl = t.toLowerCase();
    if (!TAG_BLOCKLIST.has(tl)) tagCounts[tl] = (tagCounts[tl] || 0) + 1;
  }));
  const topTags = Object.entries(tagCounts).sort((a, b) => b[1] - a[1]).slice(0, 60);

  // Status counts
  const statusCount = {};
  yd.videos.forEach(v => { const s = v.status || 'unknown'; statusCount[s] = (statusCount[s] || 0) + 1; });

  // Monthly breakdown
  const byMonth = Array(12).fill(0);
  yd.videos.forEach(v => {
    if (v.publish_date && v.publish_date.length >= 7) {
      const m = parseInt(v.publish_date.slice(5, 7), 10);
      if (m >= 1 && m <= 12) byMonth[m - 1]++;
    }
  });
  const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

  // Top videos by views & likes (full sorted lists stored for expand)
  const allByViews = [...yd.videos].filter(v => v.view_count).sort((a, b) => b.view_count - a.view_count);
  const allByLikes = [...yd.videos].filter(v => v.like_count).sort((a, b) => b.like_count - a.like_count);
  window._yrViews = allByViews;
  window._yrLikes = allByLikes;
  const topByViews = allByViews.slice(0, 5);
  const topByLikes = allByLikes.slice(0, 5);

  // Unique channels
  const uniqCh = new Set(yd.videos.map(v => v.channel_name).filter(Boolean)).size;

  panel.style.display = 'block';
  panel.innerHTML = `
  <div class="year-detail">
    <div class="year-detail-header">
      <h2>${year}</h2>
      <div class="year-kpis">
        <div class="year-kpi"><span class="kv">${yd.videos.length}</span><span class="kl">Videos</span></div>
        <div class="year-kpi"><span class="kv">${fmtBig(yd.totalViews)}</span><span class="kl">Total Views</span></div>
        <div class="year-kpi"><span class="kv">${fmtBig(yd.totalLikes)}</span><span class="kl">Total Likes</span></div>
        <div class="year-kpi"><span class="kv">${uniqCh}</span><span class="kl">Channels</span></div>
        ${topCreator ? `<div class="year-kpi"><span class="kv" style="font-size:1rem">${escHtml(topCreator[0])}</span><span class="kl">Top Creator (${topCreator[1]} videos)</span></div>` : ''}
      </div>
      <button class="close-btn" onclick="selectedYear=null;document.getElementById('year-detail-panel').style.display='none';renderYearGrid()">✕ Close</button>
    </div>

    <div class="year-charts-grid">
      <div class="chart-card">
        <h3>Videos per Month</h3>
        <canvas id="yr-month-chart" style="max-height:200px"></canvas>
      </div>
      <div class="chart-card">
        <h3>Status Breakdown</h3>
        <canvas id="yr-status-chart" style="max-height:200px"></canvas>
      </div>
      <div class="chart-card">
        <h3>Top Creators</h3>
        <canvas id="yr-creators-chart" style="max-height:200px"></canvas>
      </div>
    </div>

    ${topTags.length ? `
    <div class="chart-card" style="margin-bottom:18px">
      <h3>Tag Cloud <span style="font-size:.75rem;color:var(--text-muted);text-transform:none;letter-spacing:0">(${topTags.length} unique tags)</span></h3>
      <div class="tag-cloud-wrap" id="yr-tag-cloud"></div>
    </div>` : ''}

    <div class="charts-row cols2" style="margin-bottom:0">
      ${allByViews.length ? `
      <div class="chart-card top-videos-year">
        <h3>Top by Views <span style="font-size:.75rem;color:var(--text-muted);text-transform:none;letter-spacing:0">(${allByViews.length} total)</span></h3>
        <div class="table-wrap"><table>
          <thead><tr><th>#</th><th>Title</th><th>Views</th></tr></thead>
          <tbody id="yr-views-tbody">${yrTableRows(topByViews, 'view_count')}</tbody>
        </table></div>
        ${allByViews.length > 5 ? `<div style="text-align:center;margin-top:10px"><button class="src-expand-btn" id="yr-views-btn" onclick="expandYearTable('views')">Show all ${allByViews.length} ▼</button></div>` : ''}
      </div>` : ''}
      ${allByLikes.length ? `
      <div class="chart-card top-videos-year">
        <h3>Top by Likes <span style="font-size:.75rem;color:var(--text-muted);text-transform:none;letter-spacing:0">(${allByLikes.length} total)</span></h3>
        <div class="table-wrap"><table>
          <thead><tr><th>#</th><th>Title</th><th>Likes</th></tr></thead>
          <tbody id="yr-likes-tbody">${yrTableRows(topByLikes, 'like_count')}</tbody>
        </table></div>
        ${allByLikes.length > 5 ? `<div style="text-align:center;margin-top:10px"><button class="src-expand-btn" id="yr-likes-btn" onclick="expandYearTable('likes')">Show all ${allByLikes.length} ▼</button></div>` : ''}
      </div>` : ''}
    </div>
  </div>`;

  setTimeout(() => {
    destroyChart('yr-month'); destroyChart('yr-status'); destroyChart('yr-creators');

    charts['yr-month'] = new Chart(document.getElementById('yr-month-chart'), {
      type: 'bar',
      data: { labels: MONTHS, datasets: [{ label: 'Videos', data: byMonth, backgroundColor: PALETTE[2] + 'cc', borderRadius: 5 }] },
      options: chartOpts('')
    });

    charts['yr-status'] = new Chart(document.getElementById('yr-status-chart'), {
      type: 'doughnut',
      data: {
        labels: Object.keys(statusCount),
        datasets: [{ data: Object.values(statusCount), backgroundColor: Object.keys(statusCount).map(s => STATUS_COLORS[s] || '#888'), borderWidth: 0 }]
      },
      options: pieOpts()
    });

    const topCrSlice = topCreators.slice(0, 10);
    charts['yr-creators'] = new Chart(document.getElementById('yr-creators-chart'), {
      type: 'bar',
      data: { labels: topCrSlice.map(c => c[0]), datasets: [{ label: 'Videos', data: topCrSlice.map(c => c[1]), backgroundColor: PALETTE[0] + 'cc', borderRadius: 4 }] },
      options: {
        ...chartOpts(''), indexAxis: 'y', plugins: { legend: { display: false } },
        scales: { x: gridScales().x, y: { ...gridScales().y, ticks: { color: '#8892b0', font: { size: 9 } } } }
      }
    });

    // Tag cloud
    const cloudEl = document.getElementById('yr-tag-cloud');
    if (cloudEl && topTags.length) {
      const maxF = topTags[0][1];
      const minF = topTags[topTags.length - 1][1];
      const TAG_COLORS = ['#6c63ff', '#ff6584', '#43e97b', '#f7971e', '#38f9d7', '#fa709a', '#fee140', '#30cfd0'];
      cloudEl.innerHTML = topTags.map(([tag, freq], i) => {
        const norm = minF === maxF ? 1 : (freq - minF) / (maxF - minF);
        const size = 0.72 + norm * 1.4;
        const opacity = 0.45 + norm * 0.55;
        const color = TAG_COLORS[i % TAG_COLORS.length];
        return `<span class="cloud-tag" style="font-size:${size.toFixed(2)}rem;color:${color};opacity:${opacity.toFixed(2)};background:${color}18" title="${freq} use${freq !== 1 ? 's' : ''}">${escHtml(tag)}</span>`;
      }).join('');
    }
  }, 50);
}

// ─── SOURCES ─────────────────────────────────────────────────────────────
let sourcesPage = 1;
const SOURCES_PAGE_SIZE = 60;
const expandedSources = new Set();

function buildSourcesData() {
  // Key: source_page path. Value: { path, title, section, videos[] }
  // Uses rawVideos so source sections not in ALLOWED_SECTIONS are still visible.
  const map = {};
  rawVideos.forEach(v => {
    (v.source_pages || []).forEach((sp, i) => {
      const parts = sp.replace(/\\/g, '/').split('/');
      const section = parts.length > 1 ? parts[0] : '';
      if (!SOURCES_SECTIONS.has(section)) return;
      if (!map[sp]) {
        map[sp] = { path: sp, title: (v.thread_titles || [])[i] || sp, section, videos: [] };
      }
      map[sp].videos.push(v);
    });
  });
  return Object.values(map);
}

function initSourcesFilters(sources) {
  const sel = document.getElementById('sources-filter-section');
  if (sel.options.length > 1) return;
  const secs = [...new Set(sources.map(s => s.section).filter(Boolean))].sort();
  secs.forEach(s => {
    const o = document.createElement('option'); o.value = s; o.textContent = s;
    sel.appendChild(o);
  });
}

function renderSourcesTable() {
  const q = document.getElementById('sources-search').value.toLowerCase();
  const sec = document.getElementById('sources-filter-section').value;
  const sort = document.getElementById('sources-sort').value;

  let sources = buildSourcesData().filter(s => {
    if (sec && s.section !== sec) return false;
    if (q && !s.title.toLowerCase().includes(q) && !s.section.toLowerCase().includes(q)) return false;
    return true;
  });

  if (sort === 'videos_desc') sources.sort((a, b) => b.videos.length - a.videos.length);
  else if (sort === 'section_asc') sources.sort((a, b) => a.section.localeCompare(b.section) || a.title.localeCompare(b.title));
  else if (sort === 'title_asc') sources.sort((a, b) => a.title.localeCompare(b.title));

  document.getElementById('sources-count-label').textContent = `${sources.length} threads`;
  document.getElementById('badge-sources').textContent = buildSourcesData().length;

  const start = (sourcesPage - 1) * SOURCES_PAGE_SIZE;
  const slice = sources.slice(start, start + SOURCES_PAGE_SIZE);

  const tbody = document.getElementById('sources-tbody');
  tbody.innerHTML = slice.map(s => {
    const filePath = 'site_mirror/' + s.path.replace(/\\/g, '/');
    const rowId = 'src-' + btoa(encodeURIComponent(s.path)).replace(/[^a-z0-9]/gi, '').slice(0, 20);
    const isOpen = expandedSources.has(s.path);
    const videoRows = isOpen ? s.videos.map(v => `
      <tr class="src-video-list">
        <td>${v.id}</td>
        <td colspan="2">${v.title
        ? `<a href="${v.url}" target="_blank" style="color:var(--text);text-decoration:none">${escHtml(v.title)}</a>`
        : `<span style="opacity:.5">${escHtml(v.id)}</span>`}</td>
        <td><span class="status-dot status-${v.status || 'unavailable'}"></span>${v.status || '-'}</td>
        <td>${v.view_count != null ? fmtNum(v.view_count) + ' views' : ''}</td>
      </tr>`).join('') : '';
    return `<tr id="${rowId}">
      <td style="font-weight:600">${escHtml(s.title)}</td>
      <td><span class="tag-pill">${escHtml(s.section)}</span></td>
      <td class="num">${s.videos.length}</td>
      <td><a class="btn-thread" href="${filePath}" target="_blank">📄 Open</a></td>
      <td><button class="src-expand-btn" onclick="toggleSource(${JSON.stringify(s.path)}, '${rowId}')">${isOpen ? '▲ Hide' : '▼ Videos'}</button></td>
    </tr>${videoRows}`;
  }).join('') || `<tr><td colspan="5" class="empty">No sources match</td></tr>`;

  renderSourcesPagination(sources.length);
}

function toggleSource(path, rowId) {
  if (expandedSources.has(path)) expandedSources.delete(path);
  else expandedSources.add(path);
  renderSourcesTable();
  // scroll back to row
  const el = document.getElementById(rowId);
  if (el) el.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

function renderSourcesPagination(total) {
  const pages = Math.ceil(total / SOURCES_PAGE_SIZE);
  const el = document.getElementById('sources-pagination');
  if (pages <= 1) { el.innerHTML = ''; return; }
  let html = '';
  if (sourcesPage > 1) html += `<button class="page-btn" onclick="goSourcesPage(${sourcesPage - 1})">‹</button>`;
  const around = 2;
  for (let p = 1; p <= pages; p++) {
    if (p === 1 || p === pages || (p >= sourcesPage - around && p <= sourcesPage + around))
      html += `<button class="page-btn${p === sourcesPage ? ' active' : ''}" onclick="goSourcesPage(${p})">${p}</button>`;
    else if (p === sourcesPage - around - 1 || p === sourcesPage + around + 1)
      html += `<span class="page-info">…</span>`;
  }
  if (sourcesPage < pages) html += `<button class="page-btn" onclick="goSourcesPage(${sourcesPage + 1})">›</button>`;
  html += `<span class="page-info">${total} threads</span>`;
  el.innerHTML = html;
}

function goSourcesPage(p) { sourcesPage = p; renderSourcesTable(); window.scrollTo(0, 200); }

function yrTableRows(videos, field) {
  return videos.map((v, i) => `<tr>
    <td style="color:var(--text-muted)">${i + 1}</td>
    <td><a href="${v.url}" target="_blank" style="color:var(--text);text-decoration:none">${escHtml((v.title || v.id).slice(0, 40))}${(v.title || v.id).length > 40 ? '…' : ''}</a></td>
    <td class="num">${fmtNum(v[field])}</td>
  </tr>`).join('');
}

function expandYearTable(type) {
  const isViews = type === 'views';
  const tbody = document.getElementById(isViews ? 'yr-views-tbody' : 'yr-likes-tbody');
  const btn = document.getElementById(isViews ? 'yr-views-btn' : 'yr-likes-btn');
  const all = isViews ? window._yrViews : window._yrLikes;
  const field = isViews ? 'view_count' : 'like_count';
  const isExpanded = btn.dataset.expanded === '1';
  if (isExpanded) {
    tbody.innerHTML = yrTableRows(all.slice(0, 5), field);
    btn.textContent = `Show all ${all.length} ▼`;
    btn.dataset.expanded = '0';
  } else {
    tbody.innerHTML = yrTableRows(all, field);
    btn.textContent = `Show less ▲`;
    btn.dataset.expanded = '1';
  }
}

// ─── OVERVIEW CHARTS ──────────────────────────────────────────────────────
const PALETTE = ['#6c63ff', '#ff6584', '#43e97b', '#f7971e', '#38f9d7', '#fa709a', '#fee140', '#30cfd0', '#a18fff', '#ffecd2'];
const STATUS_COLORS = { available: '#43e97b', unavailable: '#ff6584', pending: '#f7971e', unknown: '#888' };

function buildOverview() {
  // Stat cards
  const total = allVideos.length;
  const withTitle = allVideos.filter(v => v.title).length;
  const withViews = allVideos.filter(v => v.view_count != null);
  const totalViews = withViews.reduce((s, v) => s + v.view_count, 0);
  const totalLikes = allVideos.reduce((s, v) => s + (v.like_count || 0), 0);
  const channels = new Set(allVideos.map(v => v.channel_name).filter(Boolean));
  const sections = new Set(allVideos.flatMap(v => v.sections || []));
  const available = allVideos.filter(v => v.status === 'available').length;

  document.getElementById('overview-stats').innerHTML = [
    { label: 'Total Videos', value: fmtNum(total), sub: `${withTitle} with metadata` },
    { label: 'Unique Channels', value: fmtNum(channels.size), sub: `across ${sections.size} sections` },
    { label: 'Total Views', value: fmtBig(totalViews), sub: `${withViews.length} videos with data` },
    { label: 'Total Likes', value: fmtBig(totalLikes), sub: '' },
    { label: 'Available', value: fmtNum(available), sub: `${Math.round(available / total * 100)}% of archive` },
    { label: 'Avg Views', value: withViews.length ? fmtBig(Math.round(totalViews / withViews.length)) : '-', sub: 'per video (w/ data)' },
  ].map(c => `<div class="stat-card"><div class="label">${c.label}</div><div class="value">${c.value}</div><div class="sub">${c.sub}</div></div>`).join('');

  // Videos by year
  const byYear = {};
  allVideos.forEach(v => {
    if (v.publish_date) { const y = v.publish_date.slice(0, 4); byYear[y] = (byYear[y] || 0) + 1; }
  });
  const years = Object.keys(byYear).sort();
  makeChart('chart-year', 'bar', years, [{
    label: 'Videos', data: years.map(y => byYear[y]),
    backgroundColor: years.map((_, i) => PALETTE[i % PALETTE.length] + 'bb'),
    borderRadius: 6
  }], chartOpts(''));

  // Status doughnut
  const statusCount = {};
  allVideos.forEach(v => { const s = v.status || 'unknown'; statusCount[s] = (statusCount[s] || 0) + 1; });
  makeChart('chart-status', 'doughnut',
    Object.keys(statusCount),
    [{ data: Object.values(statusCount), backgroundColor: Object.keys(statusCount).map(s => STATUS_COLORS[s] || '#888'), borderWidth: 0 }],
    pieOpts());

  // Top channels
  const chCounts = {};
  allVideos.forEach(v => { if (v.channel_name) chCounts[v.channel_name] = (chCounts[v.channel_name] || 0) + 1; });
  const topCh = Object.entries(chCounts).sort((a, b) => b[1] - a[1]).slice(0, 15);
  makeChart('chart-top-channels', 'bar', topCh.map(c => c[0]), [{
    label: 'Videos', data: topCh.map(c => c[1]),
    backgroundColor: PALETTE[0] + 'bb', borderRadius: 4
  }], { ...chartOpts(''), indexAxis: 'y', plugins: { legend: { display: false } }, scales: { x: gridScales().x, y: { ...gridScales().y, ticks: { color: '#8892b0', font: { size: 10 } } } } });

  // Sections bar
  const secCounts = {};
  allVideos.forEach(v => (v.sections || []).forEach(s => { secCounts[s] = (secCounts[s] || 0) + 1; }));
  const topSec = Object.entries(secCounts).sort((a, b) => b[1] - a[1]);
  makeChart('chart-sections', 'bar', topSec.map(s => s[0]), [{
    label: 'Videos', data: topSec.map(s => s[1]),
    backgroundColor: PALETTE[1] + 'bb', borderRadius: 4
  }], { ...chartOpts(''), indexAxis: 'y', plugins: { legend: { display: false } }, scales: { x: gridScales().x, y: { ...gridScales().y, ticks: { color: '#8892b0', font: { size: 10 } } } } });

  // Views distribution (log buckets)
  const buckets = { '0': 0, '1-100': 0, '100-1K': 0, '1K-10K': 0, '10K-100K': 0, '100K-1M': 0, '1M+': 0 };
  allVideos.forEach(v => {
    const c = v.view_count;
    if (c == null) buckets['0']++;
    else if (c < 100) buckets['1-100']++;
    else if (c < 1000) buckets['100-1K']++;
    else if (c < 10000) buckets['1K-10K']++;
    else if (c < 100000) buckets['10K-100K']++;
    else if (c < 1000000) buckets['100K-1M']++;
    else buckets['1M+']++;
  });
  makeChart('chart-views-dist', 'doughnut', Object.keys(buckets), [{
    data: Object.values(buckets),
    backgroundColor: PALETTE, borderWidth: 0
  }], pieOpts());

  // Top by views
  const topViews = allVideos.filter(v => v.view_count).sort((a, b) => b.view_count - a.view_count).slice(0, 10);
  makeChart('chart-top-views', 'bar',
    topViews.map(v => (v.title || v.id).slice(0, 25) + '…'),
    [{ label: 'Views', data: topViews.map(v => v.view_count), backgroundColor: PALETTE[2] + 'bb', borderRadius: 4 }],
    { ...chartOpts(''), indexAxis: 'y', plugins: { legend: { display: false } }, scales: { x: { ...gridScales().x, ticks: { callback: v => fmtBig(v), color: '#8892b0' } }, y: { ...gridScales().y, ticks: { color: '#8892b0', font: { size: 9 } } } } });

  // Top by likes
  const topLikes = allVideos.filter(v => v.like_count).sort((a, b) => b.like_count - a.like_count).slice(0, 10);
  makeChart('chart-top-likes', 'bar',
    topLikes.map(v => (v.title || v.id).slice(0, 25) + '…'),
    [{ label: 'Likes', data: topLikes.map(v => v.like_count), backgroundColor: PALETTE[3] + 'bb', borderRadius: 4 }],
    { ...chartOpts(''), indexAxis: 'y', plugins: { legend: { display: false } }, scales: { x: { ...gridScales().x, ticks: { callback: v => fmtBig(v), color: '#8892b0' } }, y: { ...gridScales().y, ticks: { color: '#8892b0', font: { size: 9 } } } } });
}

// ─── CHART HELPERS ────────────────────────────────────────────────────────
function makeChart(id, type, labels, datasets, options) {
  destroyChart(id);
  const ctx = document.getElementById(id);
  if (!ctx) return;
  charts[id] = new Chart(ctx, { type, data: { labels, datasets }, options });
}

function destroyChart(id) {
  if (charts[id]) { charts[id].destroy(); delete charts[id]; }
}

function gridScales() {
  return {
    x: { grid: { color: '#2a3048' }, ticks: { color: '#8892b0' } },
    y: { grid: { color: '#2a3048' }, ticks: { color: '#8892b0' } }
  };
}

function chartOpts(title) {
  return {
    responsive: true, maintainAspectRatio: true,
    plugins: {
      legend: { display: false },
      title: title ? { display: true, text: title, color: '#8892b0', font: { size: 11 } } : { display: false },
      tooltip: { backgroundColor: '#1e2435', titleColor: '#e8eaf6', bodyColor: '#8892b0', borderColor: '#2a3048', borderWidth: 1 }
    },
    scales: gridScales()
  };
}

function pieOpts() {
  return {
    responsive: true, maintainAspectRatio: true,
    plugins: {
      legend: { labels: { color: '#8892b0', font: { size: 11 }, boxWidth: 14, padding: 12 }, position: 'bottom' },
      tooltip: { backgroundColor: '#1e2435', titleColor: '#e8eaf6', bodyColor: '#8892b0' }
    }
  };
}

// ─── UTILS ────────────────────────────────────────────────────────────────
function fmtNum(n) { return n == null ? '-' : n.toLocaleString(); }
function fmtBig(n) {
  if (n == null) return '-';
  if (n >= 1e9) return (n / 1e9).toFixed(1) + 'B';
  if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
  return String(n);
}
// ─── TIMELINE ─────────────────────────────────────────────────────────────
function renderTimeline() {
  const raw = document.getElementById('tl-md').textContent;
  const html = marked.parse(raw);
  const el = document.getElementById('tl-content');
  el.innerHTML = html;
  // Colour-code era headings
  el.querySelectorAll('h2').forEach(h => {
    const m = h.textContent.match(/Era\s+(\d)/);
    if (m) h.dataset.era = m[1];
  });
  // Open all links in new tab
  el.querySelectorAll('a').forEach(a => a.target = '_blank');
}
//document.addEventListener('DOMContentLoaded', renderTimeline);

function escHtml(s) {
  if (!s) return '';
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}
function escAttr(s) { return String(s || '').replace(/'/g, "\\'").replace(/"/g, '&quot;'); }
