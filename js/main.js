(function () {
  'use strict';

  var map, markerLayer, stateLayer, legendControl;
  var table;
  var allRows = [];          // every parsed business (each tagged with .sourceState)
  var sourceCounts = {};     // source-state full name -> business count
  var coveredStates = {};    // source-state full name -> true (a state we have a data source for)
  var CONFIRMED_COLOR = '#1B4332';
  var UNVERIFIED_COLOR = '#9a9a9a';

  // ── State abbreviation ↔ full name ───────────────────────────────────────
  var STATE_NAMES = {
    AL:'Alabama',AK:'Alaska',AZ:'Arizona',AR:'Arkansas',CA:'California',CO:'Colorado',
    CT:'Connecticut',DE:'Delaware',FL:'Florida',GA:'Georgia',HI:'Hawaii',ID:'Idaho',
    IL:'Illinois',IN:'Indiana',IA:'Iowa',KS:'Kansas',KY:'Kentucky',LA:'Louisiana',
    ME:'Maine',MD:'Maryland',MA:'Massachusetts',MI:'Michigan',MN:'Minnesota',MS:'Mississippi',
    MO:'Missouri',MT:'Montana',NE:'Nebraska',NV:'Nevada',NH:'New Hampshire',NJ:'New Jersey',
    NM:'New Mexico',NY:'New York',NC:'North Carolina',ND:'North Dakota',OH:'Ohio',OK:'Oklahoma',
    OR:'Oregon',PA:'Pennsylvania',RI:'Rhode Island',SC:'South Carolina',SD:'South Dakota',
    TN:'Tennessee',TX:'Texas',UT:'Utah',VT:'Vermont',VA:'Virginia',WA:'Washington',
    WV:'West Virginia',WI:'Wisconsin',WY:'Wyoming',DC:'District of Columbia',PR:'Puerto Rico'
  };
  // Full names sorted longest-first so "West Virginia"/"South Carolina" win over "Virginia"/"Carolina".
  var STATE_FULL_DESC = Object.keys(STATE_NAMES).map(function (k) { return STATE_NAMES[k]; })
    .sort(function (a, b) { return b.length - a.length; });
  // City/other sources whose data_source string doesn't contain a state name.
  var SOURCE_CITY_STATE = [{ key: 'NYC', state: 'New York' }];

  // Which state's *program* a record comes from (for the coverage map + filter),
  // derived from its data_source label. Falls back to the business address state.
  function deriveSourceState(dataSource, addressState) {
    var ds = dataSource || '';
    for (var i = 0; i < SOURCE_CITY_STATE.length; i++) {
      if (ds.indexOf(SOURCE_CITY_STATE[i].key) !== -1) return SOURCE_CITY_STATE[i].state;
    }
    for (var j = 0; j < STATE_FULL_DESC.length; j++) {
      if (ds.indexOf(STATE_FULL_DESC[j]) !== -1) return STATE_FULL_DESC[j];
    }
    return normalizeState(addressState);
  }

  function normalizeState(s) {
    if (!s) return '';
    s = String(s).trim();
    if (s.length === 2 && STATE_NAMES[s.toUpperCase()]) return STATE_NAMES[s.toUpperCase()];
    var found = '';
    Object.keys(STATE_NAMES).forEach(function (k) {
      if (STATE_NAMES[k].toLowerCase() === s.toLowerCase()) found = STATE_NAMES[k];
    });
    return found || s;
  }

  // ── Utilities ────────────────────────────────────────────────────────────
  function escHtml(s) {
    return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }
  function safeUrl(url) { return /^https?:\/\//i.test(url) ? url : ''; }

  function confidenceBadge(conf) {
    return conf === 'confirmed_black'
      ? '<span class="badge badge-confirmed">Confirmed</span>'
      : '<span class="badge badge-unverified">MBE — unverified</span>';
  }
  function confidenceLabel(conf) {
    return conf === 'confirmed_black' ? 'Confirmed Black-owned' : 'MBE certified (ethnicity unverified)';
  }
  function markerColor(row) {
    return row.confidence === 'mbe_unverified' ? UNVERIFIED_COLOR : CONFIRMED_COLOR;
  }

  // ── Map ────────────────────────────────────────────────────────────────
  function initMap() {
    map = L.map('map', { preferCanvas: true }).setView([39.5, -98.35], 4);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
      maxZoom: 18
    }).addTo(map);
    markerLayer = L.layerGroup().addTo(map);
    addLegend();
  }

  function addLegend() {
    legendControl = L.control({ position: 'bottomright' });
    legendControl.onAdd = function () {
      var div = L.DomUtil.create('div', 'map-legend');
      div.innerHTML =
        '<div class="legend-title">Legend</div>' +
        '<div class="legend-row"><span class="legend-dot" style="background:' + CONFIRMED_COLOR + '"></span>Confirmed Black-owned</div>' +
        '<div class="legend-row"><span class="legend-dot" style="background:' + UNVERIFIED_COLOR + '"></span>MBE — unverified</div>' +
        '<div class="legend-row"><span class="legend-swatch legend-covered"></span>Source incorporated</div>' +
        '<div class="legend-row"><span class="legend-swatch legend-uncovered"></span>Not yet covered</div>';
      return div;
    };
    legendControl.addTo(map);
  }

  function addMarkersToMap(rows) {
    markerLayer.clearLayers();
    rows.forEach(function (row) {
      var lat = parseFloat(row.latitude), lon = parseFloat(row.longitude);
      if (isNaN(lat) || isNaN(lon)) return;

      var marker = L.circleMarker([lat, lon], {
        radius: 5, fillColor: markerColor(row), color: '#ffffff',
        weight: 1, opacity: 1, fillOpacity: 0.85
      });

      var desc = row.description
        ? (row.description.length > 120 ? row.description.substring(0,120) + '…' : row.description) : '';
      var href = safeUrl(row.website);
      var websiteHtml = href
        ? '<div class="popup-website"><a href="' + escHtml(href) + '" target="_blank" rel="noopener">' +
          escHtml(href.replace(/^https?:\/\//,'').split('/')[0]) + ' ↗</a></div>' : '';

      var popupHtml =
        '<div class="popup-name">' + escHtml(row.business_name) + '</div>' +
        '<div class="popup-conf">' + escHtml(confidenceLabel(row.confidence)) + '</div>' +
        '<div class="popup-meta">' + escHtml(row.industry || '') + ' · ' + escHtml(row.address_city || '') + '</div>' +
        (row.owner_name ? '<div class="popup-field"><span class="popup-label">Owner: </span>' + escHtml(row.owner_name) + '</div>' : '') +
        (row.year_founded ? '<div class="popup-field"><span class="popup-label">Founded: </span>' + escHtml(row.year_founded) + '</div>' : '') +
        (desc ? '<div class="popup-field"><span class="popup-label">About: </span>' + escHtml(desc) + '</div>' : '') +
        websiteHtml;
      marker.bindPopup(popupHtml, { maxWidth: 260 });
      marker.addTo(markerLayer);
    });
  }

  // ── State coverage layer (by source) ──────────────────────────────────────
  function styleState(feature) {
    return coveredStates[feature.properties.name]
      ? { color: CONFIRMED_COLOR, weight: 1.5, fillColor: CONFIRMED_COLOR, fillOpacity: 0.10 }
      : { color: '#cfcfcf', weight: 1, fillColor: '#000000', fillOpacity: 0.015 };
  }

  function loadStateLayer() {
    fetch('data/us-states.geojson').then(function (r) { return r.json(); }).then(function (geo) {
      stateLayer = L.geoJSON(geo, {
        style: styleState,
        onEachFeature: function (feature, layer) {
          var n = feature.properties.name;
          var c = sourceCounts[n] || 0;
          layer.bindTooltip(
            '<strong>' + escHtml(n) + '</strong><br>' +
            (coveredStates[n] ? c.toLocaleString() + ' business' + (c !== 1 ? 'es' : '') + ' in database' : 'Not yet covered'),
            { sticky: true }
          );
          layer.on('mouseover', function () { layer.setStyle({ weight: 2.5 }); });
          layer.on('mouseout', function () { stateLayer.resetStyle(layer); });
          if (coveredStates[n]) {
            layer.on('click', function () {
              var sel = document.getElementById('filter-state');
              if (sel) { sel.value = n; sel.dispatchEvent(new Event('change')); }
            });
          }
        }
      });
      stateLayer.addTo(map);
      if (markerLayer) markerLayer.bringToFront();
    }).catch(function (e) { console.error('Could not load state layer:', e); });
  }

  // ── Table ───────────────────────────────────────────────────────────────
  var EXPANDED_COLS = [7, 8, 9, 10, 11];
  function initTable(rows) {
    table = $('#business-table').DataTable({
      data: rows, pageLength: 25, lengthChange: false, autoWidth: false, dom: 'tip',
      columnDefs: [{ targets: EXPANDED_COLS, visible: false }],
      columns: [
        { data: 'business_name', title: 'Business Name' },
        { data: 'owner_name', title: 'Owner', defaultContent: '—' },
        { data: 'address_city', title: 'City', defaultContent: '—' },
        { data: 'address_state', title: 'State', defaultContent: '—' },
        { data: 'industry', title: 'Industry', defaultContent: '—' },
        { data: 'year_founded', title: 'Founded', defaultContent: '—' },
        { data: 'website', title: 'Website', defaultContent: '—', orderable: false,
          render: function (data) {
            var href = safeUrl(data); if (!href) return '—';
            return '<a href="' + escHtml(href) + '" target="_blank" rel="noopener">' +
              escHtml(href.replace(/^https?:\/\//,'').split('/')[0]) + ' ↗</a>';
          } },
        { data: 'address_street', title: 'Address', defaultContent: '—' },
        { data: 'certification', title: 'Certification', defaultContent: '—' },
        { data: 'confidence', title: 'Confidence', orderable: false,
          render: function (data) { return confidenceBadge(data); } },
        { data: 'naics_code', title: 'NAICS', defaultContent: '—' },
        { data: 'description', title: 'Description', defaultContent: '—',
          render: function (data) { return !data ? '—' : escHtml(data.length > 150 ? data.substring(0,150) + '…' : data); } }
      ]
    });
  }

  // ── Filters ─────────────────────────────────────────────────────────────
  function populateStateFilter() {
    var sel = document.getElementById('filter-state');
    if (!sel) return;
    Object.keys(coveredStates).sort().forEach(function (n) {
      var o = document.createElement('option');
      o.value = n; o.textContent = n + ' (' + sourceCounts[n].toLocaleString() + ')';
      sel.appendChild(o);
    });
  }

  function populateCityFilter(stateName) {
    var sel = document.getElementById('filter-city');
    if (!sel) return;
    sel.innerHTML = '<option value="">All cities</option>';
    if (!stateName) { sel.disabled = true; return; }
    var cities = {};
    allRows.forEach(function (r) {
      if (r.sourceState === stateName && r.address_city) cities[r.address_city] = (cities[r.address_city] || 0) + 1;
    });
    Object.keys(cities).sort().forEach(function (c) {
      var o = document.createElement('option');
      o.value = c; o.textContent = c + ' (' + cities[c] + ')';
      sel.appendChild(o);
    });
    sel.disabled = false;
  }

  function matchesText(row, q) {
    if (!q) return true;
    return (row.business_name || '').toLowerCase().indexOf(q) !== -1 ||
           (row.address_city || '').toLowerCase().indexOf(q) !== -1 ||
           (row.industry || '').toLowerCase().indexOf(q) !== -1;
  }

  function applyFilters() {
    var state = (document.getElementById('filter-state') || {}).value || '';
    var city = (document.getElementById('filter-city') || {}).value || '';
    var q = ((document.getElementById('table-search') || {}).value || '').toLowerCase();

    var filtered = allRows.filter(function (r) {
      if (state && r.sourceState !== state) return false;
      if (city && r.address_city !== city) return false;
      return matchesText(r, q);
    });

    addMarkersToMap(filtered);
    if (table) { table.clear(); table.rows.add(filtered); table.draw(); }
    var countEl = document.getElementById('result-count');
    if (countEl) countEl.textContent = filtered.length.toLocaleString() + ' shown';

    if (state && stateLayer) {
      stateLayer.eachLayer(function (layer) {
        if (layer.feature && layer.feature.properties.name === state) {
          map.fitBounds(layer.getBounds(), { padding: [20, 20] });
        }
      });
    } else if (!state) {
      map.setView([39.5, -98.35], 4);
    }
  }

  // ── Coverage bar ──────────────────────────────────────────────────────────
  var US_BLACK_EMPLOYER_BUSINESSES = 160000;
  function updateCoverageBar(count) {
    var pct = count / US_BLACK_EMPLOYER_BUSINESSES * 100;
    var fillEl = document.getElementById('coverage-fill');
    var pctEl = document.getElementById('coverage-pct');
    if (fillEl) fillEl.style.width = Math.min(pct, 100) + '%';
    if (pctEl) pctEl.textContent = pct.toFixed(1) + '%';
  }

  // ── Data loading ──────────────────────────────────────────────────────────
  function loadData() {
    Papa.parse('data/businesses.csv', {
      download: true, header: true, skipEmptyLines: true,
      complete: function (results) {
        allRows = results.data.filter(function (r) { return r.business_name; });

        sourceCounts = {}; coveredStates = {};
        allRows.forEach(function (r) {
          r.sourceState = deriveSourceState(r.data_source, r.address_state);
          if (r.sourceState) {
            sourceCounts[r.sourceState] = (sourceCounts[r.sourceState] || 0) + 1;
            coveredStates[r.sourceState] = true;
          }
        });

        var countEl = document.getElementById('stat-count');
        if (countEl) countEl.textContent = allRows.length.toLocaleString();
        var n = Object.keys(coveredStates).length;
        var coverageEl = document.getElementById('stat-coverage');
        if (coverageEl) coverageEl.textContent = n + ' State' + (n !== 1 ? 's' : '');

        loadStateLayer();
        addMarkersToMap(allRows);
        initTable(allRows);
        populateStateFilter();
        updateCoverageBar(allRows.length);
        var rc = document.getElementById('result-count');
        if (rc) rc.textContent = allRows.length.toLocaleString() + ' shown';
      },
      error: function (err) { console.error('Could not load businesses.csv:', err); }
    });
  }

  // ── Init ────────────────────────────────────────────────────────────────
  document.addEventListener('DOMContentLoaded', function () {
    initMap();
    loadData();

    document.getElementById('btn-default').addEventListener('click', function () {
      if (!table) return;
      EXPANDED_COLS.forEach(function (i) { table.column(i).visible(false); });
      this.classList.add('active');
      document.getElementById('btn-expanded').classList.remove('active');
    });
    document.getElementById('btn-expanded').addEventListener('click', function () {
      if (!table) return;
      EXPANDED_COLS.forEach(function (i) { table.column(i).visible(true); });
      this.classList.add('active');
      document.getElementById('btn-default').classList.remove('active');
    });

    document.getElementById('table-search').addEventListener('keyup', applyFilters);
    var stateSel = document.getElementById('filter-state');
    if (stateSel) stateSel.addEventListener('change', function () { populateCityFilter(this.value); applyFilters(); });
    var citySel = document.getElementById('filter-city');
    if (citySel) citySel.addEventListener('change', applyFilters);

    var reqBtn = document.getElementById('btn-request');
    if (reqBtn) reqBtn.addEventListener('click', function () {
      document.getElementById('access').scrollIntoView({ behavior: 'smooth' });
    });

    var form = document.getElementById('dataset-form');
    if (form) form.addEventListener('submit', function (e) {
      e.preventDefault();
      var btn = form.querySelector('button[type="submit"]');
      if (btn) { btn.disabled = true; btn.textContent = 'Submitting…'; }
      fetch(form.action, { method: 'POST', body: new FormData(form), headers: { 'Accept': 'application/json' } })
        .then(function (resp) {
          if (resp.ok) {
            form.style.display = 'none';
            var ok = document.getElementById('form-success');
            if (ok) ok.style.display = 'block';
          } else {
            if (btn) { btn.disabled = false; btn.textContent = 'Submit Request →'; }
            alert('Something went wrong submitting the form. Please email kylemcc@umich.edu.');
          }
        }).catch(function () {
          if (btn) { btn.disabled = false; btn.textContent = 'Submit Request →'; }
          alert('Network error. Please email kylemcc@umich.edu.');
        });
    });
  });

}());
