let allVideos = [];       // from video_index.json
let allSources = [];      // from sources_index.json
let filteredVideos = [];
let appMode = 'videos';   // 'videos' or 'sources'
let currentPage = 1;
const PAGE_SIZE = 50;
let selectedChannel = null;
let selectedSection = null;
let charts = {};

document.getElementById('fileInput').addEventListener('change', e => {
  const files = Array.from(e.target.files);
  loadMultipleFiles(files);
});

const dz = document.getElementById('dropZone');
dz.addEventListener('dragover', e => { e.preventDefault(); dz.classList.add('drag-over'); });
dz.addEventListener('dragleave', () => dz.classList.remove('drag-over'));
dz.addEventListener('drop', e => {
  e.preventDefault(); dz.classList.remove('drag-over');
  const files = Array.from(e.dataTransfer.files);
  loadMultipleFiles(files);
});

async function loadMultipleFiles(files) {
  let videoData = null;
  let sourceData = null;

  for (const f of files) {
    const text = await f.text();
    const json = JSON.parse(text);
    if (f.name.includes('sources')) sourceData = json;
    else videoData = json;
  }
  initApp(videoData, sourceData);
}

// Auto-load from same directory
async function autoLoad() {
  let vData = null;
  let sData = null;
  try {
    const rv = await fetch('video_index.json');
    if (rv.ok) vData = await rv.json();
  } catch(e) {}
  try {
    const rs = await fetch('sources_index.json');
    if (rs.ok) sData = await rs.json();
  } catch(e) {}
  initApp(vData, sData);
}
autoLoad();

// ─── INIT ─────────────────────────────────────────────────────────────────
const ALLOWED_SECTIONS = new Set(["YTP nostrane", "YTP fai da te", "YTPMV dimportazione", "YTP da internet", "Internet", "Youtube", "Scraped Channel"]);
const SOURCES_SECTIONS = new Set(["Risorse", "Tutorial per il pooping", "Old Sources"]);

function resetFilters() {
  const inputs = ['search-input', 'filter-status', 'filter-section', 'filter-channel', 'filter-views-min', 'filter-likes-min', 'filter-year', 'channel-search', 'channel-year-min', 'channel-year-max'];
  inputs.forEach(id => {
    const el = document.getElementById(id);
    if (el) el.value = (el.tagName === 'SELECT' ? '' : '');
  });
  const langSel = document.getElementById('filter-language');
  if (langSel) langSel.value = 'any';
}

function initApp(vRaw, sRaw) {
  resetFilters();
  if (vRaw) {
    allVideos = Object.entries(vRaw).map(([id, v]) => ({ id, ...v }))
      .filter(v => (v.sections || []).some(s => ALLOWED_SECTIONS.has(s)));
  }
  if (sRaw) {
    allSources = Object.entries(sRaw).map(([id, v]) => ({ id, ...v }));
  }

  if (!vRaw && !sRaw) return;

  document.getElementById('landing').style.display = 'none';
  document.getElementById('app').style.display = 'block';

  // badges
  const totalVideos = allVideos.length;
  const channels = new Set(allVideos.map(v => v.channel_name).filter(Boolean));
  const sections = new Set(allVideos.flatMap(v => v.sections || []));
  
  document.getElementById('badge-videos').textContent = totalVideos;
  document.getElementById('badge-sources').textContent = allSources.length;
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
  
  if (appMode === 'videos') {
    renderHomePage();
  }
}

// ─── NAVIGATION ──────────────────────────────────────────────────────────
function showPage(name) {
  let targetPage = name;
  if (name === 'sources' || name === 'videos') {
    appMode = name;
    targetPage = 'videos';
    document.getElementById('page-videos').querySelector('h2').textContent = appMode === 'sources' ? 'Source Search' : 'Video Search';
    buildFilterOptions();
    applyFilters();
  }

  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-tab').forEach(t => t.classList.remove('active'));
  
  // Find the correct nav tab even if targetPage is 'videos' but name is 'sources'
  const tabs = document.querySelectorAll('.nav-tab');
  tabs.forEach(t => {
    if (t.getAttribute('onclick').includes(`'${name}'`)) t.classList.add('active');
  });

  document.getElementById('page-' + targetPage).classList.add('active');
  
  if (name === 'timeline' && typeof initTimeline === 'function') {
    initTimeline();
  }
  if (name === 'youtube') {
    renderHomePage();
  }
}

// ─── THEME TOGGLES & SEARCH ────────────────────────────────────────────────
function toggleThemeMode() {
  const isOld = document.body.classList.toggle('theme-old');
  const btn = document.getElementById('toggle-modern-old');
  if (btn) btn.textContent = isOld ? 'Switch to Modern Mode' : 'Switch to Old Mode';
}

function toggleNightDay() {
  const isLight = document.body.classList.toggle('theme-light');
  document.body.classList.toggle('theme-dark', !isLight);
  const btn = document.getElementById('toggle-night-day');
  if (btn) btn.textContent = isLight ? 'Switch to Night Mode' : 'Switch to Day Mode';
}

document.querySelector('.search-button').addEventListener('click', () => {
  const q = document.getElementById('global-search-input').value.trim();
  if (q) performSearch(q);
});
document.getElementById('global-search-input').addEventListener('keypress', (e) => {
  if (e.key === 'Enter') {
    const q = e.target.value.trim();
    if (q) performSearch(q);
  }
});

function performSearch(query) {
  showPage('search');
  document.getElementById('search-query-display').textContent = query;
  
  const ytData = [...allVideos, ...allSources];
  const qLower = query.toLowerCase();
  
  // Search Channels
  const allChannels = [...new Set(ytData.map(v => v.channel_name).filter(Boolean))];
  const matchedChannels = allChannels.filter(c => c.toLowerCase().includes(qLower));
  
  const channelsContainer = document.getElementById('search-channels-results');
  if (matchedChannels.length === 0) {
    channelsContainer.innerHTML = '<p class="empty" style="padding:10px;">No channels found.</p>';
  } else {
    channelsContainer.innerHTML = matchedChannels.map(c => {
      const chVideos = ytData.filter(v => v.channel_name === c);
      const totalViews = chVideos.reduce((s, v) => s + (v.view_count || 0), 0);
      return `
        <div class="channel-card" style="display:inline-block; margin-right:15px; margin-bottom:15px; vertical-align:top; width:220px;" onclick="openProfile('${escAttr(c)}')">
          <h4>${escHtml(c)}</h4>
          <div class="ch-stats" style="margin-top:8px;">
            <span><strong>${chVideos.length}</strong> videos</span><br>
            <span><strong>${fmtNum(totalViews)}</strong> views</span>
          </div>
        </div>
      `;
    }).join('');
  }
  
  // Search Videos
  const matchedVideos = ytData.filter(v => {
    const titleMatch = (v.title || '').toLowerCase().includes(qLower);
    const idMatch = (v.id || '').toLowerCase().includes(qLower);
    const descMatch = (v.description || '').toLowerCase().includes(qLower);
    const tagMatch = (v.tags || []).some(t => t.toLowerCase().includes(qLower));
    return titleMatch || idMatch || descMatch || tagMatch;
  });
  
  matchedVideos.sort((a, b) => (b.view_count || 0) - (a.view_count || 0));
  
  const videosContainer = document.getElementById('search-videos-results');
  if (matchedVideos.length === 0) {
    videosContainer.innerHTML = '<p class="empty" style="padding:10px;">No videos found.</p>';
  } else {
    videosContainer.innerHTML = matchedVideos.slice(0, 50).map(v => renderVideoItem(v, 'list')).join('');
  }
}

// ─── YOUTUBE LOGIC ───────────────────────────────────────────────────────
function openVideo(vidId) {
  showPage('video');
  const ytData = [...allVideos, ...allSources];
  const v = ytData.find(x => x.id === vidId);
  if (!v) {
    document.getElementById('watch-title').textContent = "Video not found";
    return false;
  }

  const title = v.title || v.id;
  const channel = v.channel_name || 'Unknown Channel';
  
  document.getElementById('watch-title').textContent = title;
  document.getElementById('watch-channel').textContent = channel;
  document.getElementById('watch-channel').onclick = () => openProfile(channel);
  document.getElementById('watch-views-count').textContent = fmtNum(v.view_count || 0);
  document.getElementById('watch-date').textContent = v.publish_date ? v.publish_date.slice(0,10) : '';
  
  let desc = v.description || 'No description available.';
  document.getElementById('watch-description').textContent = desc;

  const playerContainer = document.getElementById('watch-player');
  if (v.status === 'downloaded' && v.local_file) {
    const src = getLocalVideoPath(v);
    playerContainer.innerHTML = `<video controls autoplay style="width:100%; height:390px; background:#000;">
      <source src="${src}" type="video/mp4">
    </video>`;
  } else {
    playerContainer.innerHTML = `<iframe width="100%" height="390" src="https://www.youtube-nocookie.com/embed/${v.id}?autoplay=1" allow="autoplay; encrypted-media" allowfullscreen style="border:none;"></iframe>`;
  }

  const moreContainer = document.getElementById('more-from-channel');
  if (moreContainer) {
    const moreVids = ytData.filter(x => x.channel_name === v.channel_name && x.id !== v.id).slice(0, 5);
    moreContainer.innerHTML = moreVids.map(x => renderVideoItem(x, 'list')).join('');
  }
  return false;
}

function openProfile(user) {
  showPage('profile');
  const ytData = [...allVideos, ...allSources];
  document.getElementById('profile-title').textContent = user;
  
  const userVideos = ytData.filter(v => v.channel_name === user);
  const sorted = [...userVideos].sort((a, b) => {
    return (b.publish_date || '').localeCompare(a.publish_date || '');
  });

  const statsEl = document.getElementById('profile-stats');
  if (statsEl) {
    const totalViews = userVideos.reduce((sum, v) => sum + (v.view_count || 0), 0);
    statsEl.innerHTML = `
      <strong>Channel Views:</strong> ${fmtNum(totalViews)}<br>
      <strong>Total Uploads:</strong> ${userVideos.length}<br>
      <strong>Joined:</strong> ${sorted.length > 0 && sorted[sorted.length-1].publish_date ? sorted[sorted.length-1].publish_date.slice(0,10) : 'Unknown'}
    `;
  }

  const featContainer = document.getElementById('profile-featured');
  const gridContainer = document.getElementById('profile-videos');
  
  if (sorted.length > 0) {
    const feat = sorted[0];
    featContainer.innerHTML = `
      <h3 style="margin-top:0;">${escHtml(feat.title || feat.id)}</h3>
      <iframe width="100%" height="295" src="https://www.youtube-nocookie.com/embed/${feat.id}" allow="autoplay; encrypted-media" allowfullscreen style="border:none;"></iframe>
      <p style="margin-top:10px;">${escHtml(feat.description ? feat.description.slice(0,200) + '...' : '')}</p>
    `;
    
    const others = sorted.slice(1, 13);
    gridContainer.innerHTML = others.map(v => renderVideoItem(v, 'grid')).join('');
  } else {
    featContainer.innerHTML = '';
    gridContainer.innerHTML = '';
  }
  return false;
}

function renderHomePage() {
  const ytData = [...allVideos, ...allSources];
  const featuredContainer = document.getElementById('featured-videos');
  const popularContainer = document.getElementById('popular-videos');
  const modernContainer = document.getElementById('modern-videos-grid');
  if (!featuredContainer || !popularContainer) return;

  const validVideos = ytData.filter(v => v.status === 'downloaded' || v.status === 'available');
  const sortedByDate = [...validVideos].sort((a, b) => {
    return (b.publish_date || '').localeCompare(a.publish_date || '');
  });
  const sortedByViews = [...validVideos].sort((a, b) => (b.view_count || 0) - (a.view_count || 0));

  featuredContainer.innerHTML = sortedByDate.slice(0, 5).map(v => renderVideoItem(v, 'list')).join('');
  popularContainer.innerHTML = sortedByViews.slice(0, 12).map(v => renderVideoItem(v, 'grid')).join('');
  
  if (modernContainer) {
    modernContainer.innerHTML = sortedByViews.slice(0, 24).map(v => renderModernHomeCard(v)).join('');
  }
}

function renderModernHomeCard(v) {
  const fallbackTitle = (v.thread_titles && v.thread_titles[0]) ? v.thread_titles[0] : null;
  const titleText = v.title || fallbackTitle || v.id;
  const dateText = v.publish_date ? v.publish_date.slice(0, 10) : 'Unknown Date';
  const viewsText = v.view_count != null ? fmtNum(v.view_count) + ' views' : '';
  const chText = v.channel_name || '-';
  const thumbUrl = `https://i.ytimg.com/vi/${v.id}/mqdefault.jpg`;
  const channelAvatar = 'https://upload.wikimedia.org/wikipedia/commons/8/89/Portrait_Placeholder.png';

  return `
    <div class="modern-home-card" onclick="openVideo('${v.id}')">
      <div class="yt-facade">
        <img src="${thumbUrl}" alt="Thumbnail" loading="lazy">
        <div class="play-btn"></div>
      </div>
      <div class="modern-home-info">
        <img class="channel-avatar" src="${channelAvatar}" alt="Avatar" onclick="event.stopPropagation(); openProfile('${escAttr(chText)}')">
        <div class="modern-home-text">
          <h3 class="modern-home-title" title="${escAttr(titleText)}">${escHtml(titleText)}</h3>
          <a href="#" class="modern-home-ch" onclick="event.stopPropagation(); openProfile('${escAttr(chText)}')">${escHtml(chText)}</a>
          <div class="modern-home-meta">
            ${viewsText ? `<span>${viewsText}</span><span class="dot-sep">•</span>` : ''}
            <span>${dateText}</span>
          </div>
        </div>
      </div>
    </div>
  `;
}

function renderVideoItem(v, mode = 'list') {
  const title = v.title || v.id;
  const channel = v.channel_name || 'Unknown Channel';
  const views = fmtNum(v.view_count || 0) + ' views';
  const date = v.publish_date ? v.publish_date.slice(0, 10) : 'Unknown Date';
  const thumbUrl = `https://i.ytimg.com/vi/${v.id}/mqdefault.jpg`;

  return `
    <div class="video-item ${mode}">
      <a href="#" onclick="return openVideo('${v.id}')" class="video-thumb">
        <img src="${thumbUrl}" alt="Thumbnail">
        <span class="video-time">▶</span>
      </a>
      <div class="video-info">
        <a href="#" onclick="return openVideo('${v.id}')" class="video-title" title="${escAttr(title)}">${escHtml(title)}</a>
        <div class="video-meta">
          From: <a href="#" onclick="return openProfile('${escAttr(channel)}')">${escHtml(channel)}</a><br>
          Views: ${views}<br>
          Added: ${date}
        </div>
      </div>
    </div>
  `;
}

// ─── FILTER OPTIONS ───────────────────────────────────────────────────────
// ─── FILTER OPTIONS ───────────────────────────────────────────────────────
function buildFilterOptions() {
  const sectionSel = document.getElementById('filter-section');
  const channelDatalist = document.getElementById('channel-datalist');
  const yearSel = document.getElementById('filter-year');
  
  const currentData = appMode === 'sources' ? allSources : allVideos;

  // 1. Build Sections
  const sections = [...new Set(
    currentData.flatMap(v => v.sections || [])
      .map(s => s === 'Scraped Channel' ? 'Youtube' : s)
  )]
  .filter(s => s !== 'Risorse' || appMode === 'sources')
  .sort();

  sectionSel.innerHTML = '<option value="">All Sections</option>';
  sections.forEach(s => {
    const o = document.createElement('option'); o.value = s; o.textContent = s;
    sectionSel.appendChild(o);
  });

  // 2. Build Channels
  const channels = [...new Set(currentData.map(v => v.channel_name).filter(Boolean))].sort();
  channelDatalist.innerHTML = ''; 
  channels.forEach(c => {
    const o = document.createElement('option'); o.value = c;
    channelDatalist.appendChild(o);
  });

  // 3. Build Years
  const years = [...new Set(currentData.map(v => v.publish_date ? v.publish_date.slice(0, 4) : null).filter(Boolean))].sort();
  yearSel.innerHTML = '<option value="">All Years</option>';
  years.forEach(y => {
    const o = document.createElement('option'); o.value = y; o.textContent = y;
    yearSel.appendChild(o);
  });
}
// ─── FILTERS + TABLE ──────────────────────────────────────────────────────
let sortField = 'publish_date';
let sortDir = 1;
let scrollObserver = null;
let viewMode = 'table';

function setViewMode(mode) {
  viewMode = mode;
  document.getElementById('btn-view-table').classList.toggle('active', mode === 'table');
  document.getElementById('btn-view-grid').classList.toggle('active', mode === 'grid');
  document.getElementById('video-table-wrap').style.display = mode === 'table' ? 'block' : 'none';
  document.getElementById('video-grid').style.display = mode === 'grid' ? 'grid' : 'none';
  renderTable(false);
}

function loadFacade(id) {
  const el = document.getElementById('facade-' + id);
  if (!el) return;

  const currentData = appMode === 'sources' ? allSources : allVideos;
  const v = currentData.find(x => x.id === id);

  if (v && v.status === 'downloaded' && v.local_file) {
    const src = getLocalVideoPath(v);
    el.innerHTML = `<video controls autoplay style="width:100%; height:100%; object-fit:contain; background:#000;">
      <source src="${src}" type="video/mp4">
      Your browser does not support the video tag.
    </video>`;
  } else {
    el.innerHTML = `<iframe src="https://www.youtube-nocookie.com/embed/${id}?autoplay=1" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>`;
  }
}

function applyFilters() {
  const q = document.getElementById('search-input').value.toLowerCase().trim();
  const status = document.getElementById('filter-status').value;
  const section = document.getElementById('filter-section').value;
  const channel = document.getElementById('filter-channel').value;
  const viewsMin = parseInt(document.getElementById('filter-views-min').value) || 0;
  const likesMin = parseInt(document.getElementById('filter-likes-min').value) || 0;
  const year = document.getElementById('filter-year').value;
  const langSelect = document.getElementById('filter-language');
  const selectedLangs = Array.from(langSelect.selectedOptions).map(opt => opt.value.toLowerCase());
  const currentData = appMode === 'sources' ? allSources : allVideos;

  filteredVideos = currentData.filter(v => {
    // 1. IMPROVED SEARCH BAR LOGIC
    if (q) {
      const haystack = [
        v.id,
        v.title,
        v.channel_name,
        v.description,
        ...(v.thread_titles || []),
        ...(v.tags || [])
      ].join(' ').toLowerCase();

      const searchTerms = q.split(/\s+/);
      const matchesAllTerms = searchTerms.every(term => haystack.includes(term));
      if (!matchesAllTerms) return false;
    }

    if (status && v.status !== status) return false;
    if (section && !(v.sections || []).includes(section)) return false;
    if (channel && (!v.channel_name || v.channel_name.toLowerCase() !== channel.toLowerCase())) return false;
    if (viewsMin && (v.view_count || 0) < viewsMin) return false;
    if (likesMin && (v.like_count || 0) < likesMin) return false;
    if (year && (!v.publish_date || !v.publish_date.startsWith(year))) return false;
    if (selectedLangs.length > 0 && !selectedLangs.includes("Any".toLocaleLowerCase())) {
      const vidLang = (v.language || "").toLowerCase();
      if (!selectedLangs.includes(vidLang)) return false;
    }
    return true;
  });

  // 3. Sorting Logic
filteredVideos.sort((a, b) => {
  let av = a[sortField] || '';
  let bv = b[sortField] || '';
  // Fix: Check types independently
  if (typeof av === 'string') av = av.toLowerCase();
  if (typeof bv === 'string') bv = bv.toLowerCase();
  
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
if (typeof av === 'string') av = av.toLowerCase();
if (typeof bv === 'string') bv = bv.toLowerCase();
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
  const grid = document.getElementById('video-grid');
  const total = filteredVideos.length;
  document.getElementById('videos-count-label').textContent = `${total} ${appMode === 'sources' ? 'sources' : 'videos'}`;

  const start = (currentPage - 1) * PAGE_SIZE;
  const slice = filteredVideos.slice(start, start + PAGE_SIZE);

  if (viewMode === 'table') {
    const html = slice.map(v => {
      const statusClass = 'status-' + (v.status || 'unavailable');

      const threads = (v.source_pages || [])
        // 1. Check if the string includes 'channel_scrape' and exclude it if it does
        .filter(sp => !sp.includes('channel_scrape'))
        // 2. Map over whatever is left to create your links
        .map((sp, i) => {
          const path = 'https://raw.githubusercontent.com/nocoldiz/ytpbackup/main/site_mirror/' + sp.replace(/\\/g, '/');


          const label = (v.thread_titles || [])[i] || sp;


          return `<a class="btn-thread" href="${path}" onclick="downloadFile('${path}', '${escAttr(label)}.html')" title="${escHtml(label)}">📄 ${escHtml(label.length > 22 ? label.slice(0, 22) + '…' : label)}</a>`;
        }).join(' ');
      const sections = (v.sections || []).map(s => `<span class="tag-pill">${escHtml(s)}</span>`).join('');

      // Determine the title to display, falling back to the first thread title if v.title is missing
      const fallbackTitle = (v.thread_titles && v.thread_titles[0]) ? v.thread_titles[0] : null;
      const titleContent = v.title
        ? `<a href="${v.url}" target="_blank">${escHtml(v.title)}</a>`
        : (fallbackTitle ? `<a href="${v.url}" target="_blank"><em>${escHtml(fallbackTitle)}</em></a>` : `<span class="vid-id">${v.id}</span>`);

      const playAction = (v.status === 'downloaded' && v.local_file)
        ? `<a class="btn-play" href="${getLocalVideoPath(v)}" target="_blank" title="Play local file">▶</a>`
        : '';

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
          <td>${playAction} <a class="btn-yt" href="${v.url}" target="_blank">YT</a></td>
        </tr>`;
    }).join('') || (append ? '' : `<tr><td colspan="8" class="empty">No videos match your filters</td></tr>`);

    if (append) {
      tbody.insertAdjacentHTML('beforeend', html);
    } else {
      tbody.innerHTML = html;
    }
  } else {
    const html = slice.map(v => {
      const statusClass = 'status-' + (v.status || 'unavailable');
      const fallbackTitle = (v.thread_titles && v.thread_titles[0]) ? v.thread_titles[0] : null;
      const titleText = v.title || fallbackTitle || v.id;
      const dateText = v.publish_date ? v.publish_date.slice(0, 10) : 'Unknown Date';
      const viewsText = v.view_count != null ? fmtNum(v.view_count) + ' views' : '';
      const chText = v.channel_name || '-';

      const thumbUrl = `https://i.ytimg.com/vi/${v.id}/mqdefault.jpg`;
      const facadeHtml = v.status === 'available' || v.status === 'pending' || v.status === 'downloaded'
        ? `<div class="yt-facade" id="facade-${v.id}" onclick="loadFacade('${v.id}')">
             <img src="${thumbUrl}" alt="Thumbnail" loading="lazy">
             <div class="play-btn"></div>
           </div>`
        : `<div class="yt-facade" style="background:#2a3048; display:flex; align-items:center; justify-content:center; color:var(--text-muted); cursor:default;">
             <span style="opacity:0.5">Thumbnail Unavailable</span>
           </div>`;

      return `<div class="vid-card">
        ${facadeHtml}
        <div class="vid-card-info">
          <a href="${v.url}" target="_blank" class="vid-card-title" title="${escAttr(titleText)}">${escHtml(titleText)}</a>
          <a href="${v.channel_url || '#'}" target="_blank" class="vid-card-ch">${escHtml(chText)}</a>
          <div class="vid-card-meta">
            ${viewsText ? `<span>${viewsText}</span>` : ''}
            <span>${dateText}</span>
          </div>
          <div class="vid-status-row">
            <span class="status-dot ${statusClass}"></span>
            <span class="status-text">${v.status || '-'}</span>
          </div>
        </div>
      </div>`;
    }).join('') || (append ? '' : `<div class="empty" style="grid-column:1/-1">No videos match your filters</div>`);

    if (append) grid.insertAdjacentHTML('beforeend', html);
    else grid.innerHTML = html;
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
    if (!map[ch]) map[ch] = { name: ch, url: v.channel_url, videos: [], totalViews: 0, totalLikes: 0, firstYear: null };
    map[ch].videos.push(v);
    map[ch].totalViews += v.view_count || 0;
    map[ch].totalLikes += v.like_count || 0;
    const y = v.publish_date ? parseInt(v.publish_date.slice(0, 4)) : null;
    if (y) {
      if (!map[ch].firstYear || y < map[ch].firstYear) map[ch].firstYear = y;
    }
  });
  return Object.values(map).sort((a, b) => b.videos.length - a.videos.length);
}

function renderChannelGrid() {
  const q = (document.getElementById('channel-search').value || '').toLowerCase();
  const minYear = parseInt(document.getElementById('channel-year-min').value) || null;
  const maxYear = parseInt(document.getElementById('channel-year-max').value) || null;

  const channels = buildChannelData().filter(c => {
    if (q && !c.name.toLowerCase().includes(q)) return false;
    if (minYear && (!c.firstYear || c.firstYear < minYear)) return false;
    if (maxYear && (!c.firstYear || c.firstYear > maxYear)) return false;
    return true;
  });
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
    const playAction = (v.status === 'downloaded' && v.local_file)
      ? `<a class="btn-play" href="${getLocalVideoPath(v)}" target="_blank" title="Play local file">▶</a>`
      : '';

    return `<tr>
      <td class="title-cell"><a href="${v.url}" target="_blank" style="color:var(--text);text-decoration:none">${escHtml(v.title || v.id)}</a></td>
      <td>${v.publish_date ? v.publish_date.slice(0, 10) : '-'}</td>
      <td><span class="status-dot ${statusClass}"></span><span class="status-text">${v.status || '-'}</span></td>
      <td class="num">${fmtNum(v.view_count)}</td>
      <td class="num">${fmtNum(v.like_count)}</td>
      <td>${playAction} <a class="btn-yt" href="${v.url}" target="_blank">YT</a></td>
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

function getLocalVideoPath(v) {
  if (!v.local_file) return '';
  let path = v.local_file.replace(/\\/g, '/');

  // If the path starts with a generic section folder, try to use the channel folder instead.
  const genericFolders = ["Risorse", "Old sources", "Tutorial per il pooping", "Tutorial"];
  for (const folder of genericFolders) {
    if (path.startsWith(`videos/${folder}/`)) {
      if (v.channel_name) {
        let safeCh = v.channel_name.replace(/[<>:"/\\|?*]/g, '_');
        safeCh = safeCh.replace(/\s+/g, ' ').trim().slice(0, 80);
        path = path.replace(`videos/${folder}/`, `videos/${safeCh}/`);
      }
      break;
    }
  }

  return path.startsWith('videos/') ? '../' + path : path;
}

function escHtml(s) {
  if (!s) return '';
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}
function escAttr(s) { return String(s || '').replace(/'/g, "\\'").replace(/"/g, '&quot;'); }

function downloadFile(url, filename) {
  if (event) event.preventDefault();
  fetch(url)
    .then(r => r.blob())
    .then(blob => {
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(a.href);
    })
    .catch(err => {
      console.error('Download failed:', err);
      window.open(url, '_blank');
    });
}
// ─── TIMELINE ENGINE ────────────────────────────────────────────────────────

let ts = {
  initialized: false,
  msPerPixel: 0,
  centerTime: 0,
  minTime: new Date(2007, 0, 1).getTime(),
  maxTime: new Date(new Date().getFullYear(), 11, 31).getTime(), // End of current year
  isDragging: false,
  startY: 0,
  startCenterTime: 0
};

function initTimeline() {
  if (ts.initialized) return;
  const container = document.getElementById('timeline-container');
  if (!container || container.clientHeight === 0) return;

  const height = container.clientHeight;

  // Initial zoom: Fit the whole span from 2007 to Now inside the viewport
  ts.msPerPixel = (ts.maxTime - ts.minTime) / height;
  ts.centerTime = ts.minTime + (ts.maxTime - ts.minTime) / 2;

  // Wheel Zoom Event
  container.addEventListener('wheel', (e) => {
    e.preventDefault();
    const zoomFactor = e.deltaY > 0 ? 1.1 : 0.9;
    const rect = container.getBoundingClientRect();
    const cursorY = e.clientY - rect.top;

    // Calculate time directly under cursor to zoom into that specific point
    const timeAtCursor = ts.centerTime + (cursorY - rect.height / 2) * ts.msPerPixel;

    ts.msPerPixel *= zoomFactor;

    // Zoom Limits (Max Out = Fit All, Max In = 100px per day)
    const minMsPerPx = (1000 * 60 * 60 * 24) / 100;
    const maxMsPerPx = (ts.maxTime - ts.minTime) / rect.height;
    ts.msPerPixel = Math.max(minMsPerPx, Math.min(maxMsPerPx, ts.msPerPixel));

    // Readjust center so the point under cursor doesn't jump
    ts.centerTime = timeAtCursor - (cursorY - rect.height / 2) * ts.msPerPixel;

    clampTimeline(rect.height);
    renderTimelineView();
  }, { passive: false });

  // Mouse Drag Panning Events
  container.addEventListener('mousedown', e => {
    ts.isDragging = true;
    ts.startY = e.clientY;
    ts.startCenterTime = ts.centerTime;
  });

  window.addEventListener('mousemove', e => {
    if (!ts.isDragging) return;
    const dy = e.clientY - ts.startY;
    ts.centerTime = ts.startCenterTime - dy * ts.msPerPixel;
    clampTimeline(container.clientHeight);
    renderTimelineView();
  });

  window.addEventListener('mouseup', () => ts.isDragging = false);
  window.addEventListener('mouseleave', () => ts.isDragging = false);

  ts.initialized = true;
  renderTimelineView();
}

function clampTimeline(height) {
  const visibleHalf = (height / 2) * ts.msPerPixel;
  ts.centerTime = Math.max(ts.minTime + visibleHalf, Math.min(ts.maxTime - visibleHalf, ts.centerTime));
}

function renderTimelineView() {
  const container = document.getElementById('timeline-container');
  const track = document.getElementById('timeline-track');
  const height = container.clientHeight;
  track.innerHTML = ''; // Clear DOM nodes - lazy loading

  // Identify exact timestamp boundaries currently visible
  const startVisibleTime = ts.centerTime - (height / 2) * ts.msPerPixel;
  const endVisibleTime = ts.centerTime + (height / 2) * ts.msPerPixel;

  // zoomRatio: 1 is fully zoomed out (decades), near 0 is zoomed in (days)
  const maxMsPerPx = (ts.maxTime - ts.minTime) / height;
  const zoomRatio = ts.msPerPixel / maxMsPerPx;

  // Show only milestones (> 10M) when heavily zoomed out (> 15% of total time span visible)
  const showOnlyMilestones = zoomRatio > 0.15;

  drawTimelineMarkers(startVisibleTime, endVisibleTime, zoomRatio, track);

  // 1. Filter out videos that aren't visible or lack dates
  const visibleVideos = allVideos.filter(v => {
    if (!v.publish_date) return false;
    const t = new Date(v.publish_date).getTime();
    return t >= startVisibleTime && t <= endVisibleTime;
  });

  const placed = [];
  const PIXEL_GAP = 55; // Vertical space required to prevent cards overlapping

  // 2. Render visible videos
  visibleVideos.forEach(v => {
    // Zoom enforcement constraint
    if (showOnlyMilestones && (v.view_count || 0) < 10000000) return;

    const t = new Date(v.publish_date).getTime();
    const yPos = (t - startVisibleTime) / ts.msPerPixel;

    // Layout logic: if a video shares a date with another, push it right (multi-columns)
    let col = 0;
    while (placed.some(p => p.col === col && Math.abs(p.top - yPos) < PIXEL_GAP)) {
      col++;
    }
    placed.push({ top: yPos, col });

    const el = document.createElement('div');
    const isMilestone = (v.view_count || 0) >= 10000000;
    el.className = 'timeline-event' + (isMilestone ? ' milestone' : '');

    el.style.top = `${yPos}px`;
    el.style.left = `${100 + col * 260}px`; // Indent based on column
    el.onclick = () => window.open(v.url || `https://youtube.com/watch?v=${v.id}`, '_blank');

    const views = v.view_count ? ` • ${fmtNum(v.view_count)} views` : '';
    el.innerHTML = `
      <strong>${escHtml(v.title || v.id)}</strong>
      <small>${escHtml(v.channel_name || 'Unknown')} • ${v.publish_date.slice(0, 10)}${views}</small>
    `;
    track.appendChild(el);
  });
}

function drawTimelineMarkers(startVisibleTime, endVisibleTime, zoomRatio, track) {
  // Determine granularity based on zoom level
  let intervalType = 'year';
  if (zoomRatio < 0.02) intervalType = 'day';
  else if (zoomRatio < 0.15) intervalType = 'month';

  let current = new Date(startVisibleTime);

  // Round up to nearest interval cleanly
  if (intervalType === 'year') {
    current.setMonth(0, 1); current.setHours(0, 0, 0, 0);
  } else if (intervalType === 'month') {
    current.setDate(1); current.setHours(0, 0, 0, 0);
  } else {
    current.setHours(0, 0, 0, 0);
  }

  while (current.getTime() <= endVisibleTime) {
    if (current.getTime() >= startVisibleTime) {
      const yPos = (current.getTime() - startVisibleTime) / ts.msPerPixel;
      const el = document.createElement('div');
      el.className = 'timeline-marker';
      el.style.top = `${yPos}px`;

      // Scale label output
      if (intervalType === 'year') {
        el.textContent = current.getFullYear();
      } else if (intervalType === 'month') {
        el.textContent = current.toLocaleString('default', { month: 'long', year: 'numeric' });
      } else {
        el.textContent = current.toLocaleDateString();
      }
      track.appendChild(el);
    }

    // Increment cursor
    if (intervalType === 'year') current.setFullYear(current.getFullYear() + 1);
    else if (intervalType === 'month') current.setMonth(current.getMonth() + 1);
    else current.setDate(current.getDate() + 1);
  }
}