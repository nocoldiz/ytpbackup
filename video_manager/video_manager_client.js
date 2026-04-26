'use strict';

let allVideos = [];
let filteredVideos = [];
let selectedIds = new Set();
let currentPage = 1;
const PAGE_SIZE = 100;

// ─── LOADING ─────────────────────────────────────────────────────────────
fetch('/docs/video_index.json')
  .then(r => r.json())
  .then(data => {
    allVideos = Object.entries(data).map(([id, v]) => ({ id, ...v }));
    initApp();
  })
  .catch(err => console.error('Failed to load video index:', err));

function initApp() {
  buildFilterOptions();
  applyFilters();
  setupEventListeners();
  setupScrollObserver();
}

function setupEventListeners() {
  document.getElementById('search-input').addEventListener('input', () => { currentPage = 1; applyFilters(); });
  document.getElementById('filter-status').addEventListener('change', () => { currentPage = 1; applyFilters(); });
  document.getElementById('filter-section').addEventListener('change', () => { currentPage = 1; applyFilters(); });
  
  document.getElementById('select-all').addEventListener('change', e => {
    const isChecked = e.target.checked;
    const checkboxes = document.querySelectorAll('.video-checkbox');
    checkboxes.forEach(cb => {
      cb.checked = isChecked;
      if (isChecked) selectedIds.add(cb.dataset.id);
      else selectedIds.delete(cb.dataset.id);
    });
    updateSelectionUI();
  });

  document.getElementById('btn-ban-selected').addEventListener('click', banSelected);
}

function buildFilterOptions() {
  const sectionSel = document.getElementById('filter-section');
  const sections = [...new Set(allVideos.flatMap(v => v.sections || []))].sort();
  sections.forEach(s => {
    const o = document.createElement('option'); o.value = s; o.textContent = s;
    sectionSel.appendChild(o);
  });
}

function applyFilters() {
  const q = document.getElementById('search-input').value.toLowerCase().trim();
  const status = document.getElementById('filter-status').value;
  const section = document.getElementById('filter-section').value;

  filteredVideos = allVideos.filter(v => {
    if (q) {
      const haystack = [v.id, v.title, v.channel_name, ...(v.tags || [])].join(' ').toLowerCase();
      if (!haystack.includes(q)) return false;
    }
    if (status && v.status !== status) return false;
    if (section && !(v.sections || []).includes(section)) return false;
    return true;
  });

  renderTable(false);
}

function renderTable(append = false) {
  const tbody = document.getElementById('video-tbody');
  const total = filteredVideos.length;
  document.getElementById('videos-count-label').textContent = `${total} videos`;

  const start = (currentPage - 1) * PAGE_SIZE;
  const slice = filteredVideos.slice(start, start + PAGE_SIZE);

  const html = slice.map(v => {
    const isChecked = selectedIds.has(v.id) ? 'checked' : '';
    return `
      <tr>
        <td class="select-col"><input type="checkbox" class="video-checkbox" data-id="${v.id}" ${isChecked} onchange="toggleSelect('${v.id}', this.checked)"></td>
        <td class="title-cell">
          <strong>${esc(v.title || 'No Title')}</strong>
          <div class="vid-id">${v.id}</div>
        </td>
        <td>${esc(v.channel_name || '-')}</td>
        <td>${v.publish_date ? v.publish_date.slice(0, 10) : '-'}</td>
        <td><span class="status-dot status-${v.status}"></span> ${v.status || '-'}</td>
        <td><a class="btn-yt" href="${v.url}" target="_blank">YT</a></td>
      </tr>
    `;
  }).join('');

  if (append) {
    tbody.insertAdjacentHTML('beforeend', html);
  } else {
    tbody.innerHTML = html || '<tr><td colspan="6" class="empty">No videos found</td></tr>';
  }
}

window.toggleSelect = function(id, isChecked) {
  if (isChecked) selectedIds.add(id);
  else selectedIds.delete(id);
  updateSelectionUI();
};

function updateSelectionUI() {
  const count = selectedIds.size;
  document.getElementById('selection-summary').textContent = `${count} videos selected`;
  document.getElementById('btn-ban-selected').disabled = count === 0;
}

function setupScrollObserver() {
  const sentinel = document.getElementById('scroll-sentinel');
  const observer = new IntersectionObserver(entries => {
    if (entries[0].isIntersecting && currentPage * PAGE_SIZE < filteredVideos.length) {
      currentPage++;
      renderTable(true);
    }
  }, { rootMargin: '200px' });
  observer.observe(sentinel);
}

async function banSelected() {
  const ids = Array.from(selectedIds);
  if (!confirm(`Are you sure you want to ban ${ids.length} videos? This will delete local files and remove them from the index.`)) return;

  const btn = document.getElementById('btn-ban-selected');
  btn.disabled = true;
  btn.textContent = 'Banning...';

  try {
    const r = await fetch('/api/ban', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ videoIds: ids })
    });
    const res = await r.json();

    if (res.success) {
      alert(`Successfully banned ${res.results.deleted.length} videos.`);
      // Reload or update local state
      location.reload();
    } else {
      alert('Error: ' + res.error);
    }
  } catch (err) {
    alert('Failed to connect to server.');
  } finally {
    btn.disabled = false;
    btn.textContent = 'Ban Selected Videos';
  }
}

function esc(str) {
  if (!str) return '';
  return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}
