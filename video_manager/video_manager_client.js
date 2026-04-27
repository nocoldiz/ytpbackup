'use strict';

let allVideos = [];
let filteredVideos = [];
let selectedIds = new Set();
let currentPage = 1;
let filterNonYtp = false;
const PAGE_SIZE = 100;

const NON_YTP_KEYWORDS = /Walkthrough|Playthrough|Let's\s+Play|Gameplay|Longplay|No\s+Commentary|Speedrun|Boss\s+Fight|Achievement\s+Guide|Trophy\s+Guide|100%\s+Completion|Quest\s+Line|Partita|Giocata|Commento|Reazione|Reaction\s+ita|Dal\s+vivo|Streaming\s+ora|Migliori\s+momenti|Highlights\s+live|Torneo|Guida\s+completa|Unboxing|Review|Hands-on|Benchmark|Comparison|Specs|Tech\s+News|Setup|Hardware|Software\s+Tutorial|How\s?to\s+Install|Step\s+by\s+Step|Buying\s+Guide|Recensione|Prova|Test|Recensione\s+Onesta|Confronto|Loquendo|Cosa\s+ne\s+penso|Consigli\s+per\s+gli\s+acquisti|Scheda\s+Video|Vlog|Daily\s+Routine|GRWM|Get\s+Ready\s+With\s+Me|Haul|Q&A|Ask\s+Me\s+Anything|Lifestyle|Life\s+Updates|Day\s+in\s+the\s+life|Travel\s+Diary|La\s+mia\s+routine|Cosa\s+mangio|Vlog\s+ita|Viaggio\s+a|Domande\s+e\s+risposte|Le\s+mie\s+opinioni|Draw\s+my\s+life\s+ita|Challenge\s+ita|Official\s+Music\s+Video|Lyric\s+Video|Sountrack|OST|Official\s+Trailer|Teaser\s+Trailer|Full\s+Episode|News\s+Report|Breaking\s+News|Press\s+Conference|Short\s+Film|Behind\s+the\s+Scenes|BTS|Making\s+of|Puntata\s+intera|Episodio\s+completo|Film\s+completo|Versione\s+integrale|Video\s+ufficiale|Audio\s+ufficiale|Sigla|Testo\s+canzone|Trailer\s+italiano|Servizio|Conferenza\s+stampa|Reportage|Lecture|Webinar|Course|Seminar|Presentation|Keynote|Workshop|Tutorial\s+for\s+beginners|Masterclass|Podcast\s+Episode|TED\s?Talk|Tutorial\s+ita|Come\s+fare|Spiegazione|Lezione|Corso\s+di|ASMR|Meditation|Workout|Fitness\s+Routine|Recipe|Cooking\s+Class|DIY\s+Crafts|Fai\s+da\s+te/i;

// ─── LOADING ─────────────────────────────────────────────────────────────
Promise.all([
  fetch('/docs/video_index.json').then(r => r.json()),
  fetch('/docs/sources_index.json').then(r => r.json())
]).then(([videoData, sourceData]) => {
  const v1 = Object.entries(videoData).map(([id, v]) => ({ id, ...v, isSource: false }));
  const v2 = Object.entries(sourceData).map(([id, v]) => ({ id, ...v, isSource: true }));
  allVideos = [...v1, ...v2];
  initApp();
}).catch(err => console.error('Failed to load indices:', err));

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
  
  document.getElementById('btn-filter-non-ytp').addEventListener('click', e => {
    filterNonYtp = !filterNonYtp;
    e.target.classList.toggle('active', filterNonYtp);
    currentPage = 1;
    applyFilters();
  });
  
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
  document.getElementById('btn-flag-source-selected').addEventListener('click', flagSourceSelected);
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
    if (filterNonYtp && !NON_YTP_KEYWORDS.test(v.title || '')) return false;
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
        <td>
          <span class="status-dot status-${v.status}"></span> ${v.status || '-'}
          ${v.isSource ? '<span class="badge-source">Source</span>' : ''}
        </td>
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
  
  // Disable "Flag as Source" if any already flagged or if none selected
  const anySourceSelected = Array.from(selectedIds).some(id => {
    const v = allVideos.find(v => v.id === id);
    return v && v.isSource;
  });
  document.getElementById('btn-flag-source-selected').disabled = count === 0 || anySourceSelected;
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

async function flagSourceSelected() {
  const ids = Array.from(selectedIds);
  if (!confirm(`Are you sure you want to flag ${ids.length} videos as sources? This will move them to the sources index.`)) return;

  const btn = document.getElementById('btn-flag-source-selected');
  btn.disabled = true;
  btn.textContent = 'Flagging...';

  try {
    const r = await fetch('/api/flag-source', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ videoIds: ids })
    });
    const res = await r.json();

    if (res.success) {
      alert(`Successfully flagged ${res.results.moved.length} videos as sources.`);
      location.reload();
    } else {
      alert('Error: ' + res.error);
    }
  } catch (err) {
    alert('Failed to connect to server.');
  } finally {
    btn.disabled = false;
    btn.textContent = 'Flag as Source';
  }
}

function esc(str) {
  if (!str) return '';
  return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}
