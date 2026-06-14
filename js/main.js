(function () {
  'use strict';

  // ── Mapbox token ──────────────────────────────────────────────────────────
  // Paste your public Mapbox token below, then restrict it by URL in
  // Mapbox account settings → Tokens → Allowed URLs.
  var MAPBOX_TOKEN = 'pk.eyJ1Ijoia3lsZW1jY3VsbGVycyIsImEiOiJjbXFlMmY2ZGMxNnc0MnJvZ2k0bnE0aWV5In0.6MIiC0eq67WYPLesyliFAQ';
  mapboxgl.accessToken = MAPBOX_TOKEN;

  // ── Constants ─────────────────────────────────────────────────────────────
  var CONFIRMED_COLOR    = '#1B4332';
  var UNVERIFIED_COLOR   = '#9a9a9a';
  var COVERED_FILL_COLOR = '#1B4332';
  var COVERED_FILL_OPACITY   = 0.10;
  var COVERED_LINE_COLOR     = '#1B4332';
  var COVERED_LINE_OPACITY   = 0.7;
  var UNCOVERED_FILL_COLOR   = '#cccccc';
  var UNCOVERED_FILL_OPACITY = 0.08;
  var UNCOVERED_LINE_COLOR   = '#cccccc';
  var UNCOVERED_LINE_OPACITY = 0.4;

  var US_CENTER = [-98.35, 39.5];
  var US_ZOOM   = 3.3;

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
  var SOURCE_CITY_STATE = [
    { key: 'NYC', state: 'New York' },
    { key: 'Houston', state: 'Texas' },
    { key: 'Atlanta', state: 'Georgia' },
    { key: 'Chicago', state: 'Illinois' },
    { key: 'Baltimore', state: 'Maryland' }
  ];

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

  // ── Utilities ─────────────────────────────────────────────────────────────
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

  // ── Module state ──────────────────────────────────────────────────────────
  var map;
  var table;
  var allRows    = [];
  var sourceCounts  = {};   // source-state full name -> business count
  var coveredStates = {};   // source-state full name -> true
  var activePopup       = null;
  var stateHoverPopup   = null;
  var hoveredStateId    = null;

  // ── Map init ──────────────────────────────────────────────────────────────
  function initMap(onReady) {
    map = new mapboxgl.Map({
      container: 'map',
      style: 'mapbox://styles/mapbox/light-v11',
      center: US_CENTER,
      zoom: US_ZOOM,
      attributionControl: true
    });

    map.addControl(new mapboxgl.NavigationControl({ showCompass: false }), 'top-right');

    map.on('load', function () {

      // ── States source ────────────────────────────────────────────────────
      map.addSource('us-states', {
        type: 'geojson',
        data: 'data/us-states.geojson',
        generateId: true   // assigns feature.id for feature-state hover
      });

      // ── State fill layer ─────────────────────────────────────────────────
      map.addLayer({
        id: 'states-fill',
        type: 'fill',
        source: 'us-states',
        paint: {
          'fill-color': COVERED_FILL_COLOR,
          'fill-opacity': [
            'case',
            ['boolean', ['feature-state', 'covered'], false],
            COVERED_FILL_OPACITY,
            UNCOVERED_FILL_OPACITY
          ]
        }
      });

      // ── State hover highlight ────────────────────────────────────────────
      map.addLayer({
        id: 'states-fill-hover',
        type: 'fill',
        source: 'us-states',
        paint: {
          'fill-color': COVERED_FILL_COLOR,
          'fill-opacity': [
            'case',
            ['boolean', ['feature-state', 'hovered'], false],
            0.18,
            0
          ]
        }
      });

      // ── State outline layer ──────────────────────────────────────────────
      map.addLayer({
        id: 'states-line',
        type: 'line',
        source: 'us-states',
        paint: {
          'line-color': [
            'case',
            ['boolean', ['feature-state', 'covered'], false],
            COVERED_LINE_COLOR,
            UNCOVERED_LINE_COLOR
          ],
          'line-opacity': [
            'case',
            ['boolean', ['feature-state', 'covered'], false],
            COVERED_LINE_OPACITY,
            UNCOVERED_LINE_OPACITY
          ],
          'line-width': [
            'case',
            ['boolean', ['feature-state', 'covered'], false],
            1.5,
            0.5
          ]
        }
      });

      // ── Business dots source (empty initially) ───────────────────────────
      map.addSource('businesses', {
        type: 'geojson',
        data: { type: 'FeatureCollection', features: [] }
      });

      // ── Business dots layer ──────────────────────────────────────────────
      map.addLayer({
        id: 'businesses-circles',
        type: 'circle',
        source: 'businesses',
        paint: {
          'circle-radius': 4.5,
          'circle-color': [
            'match',
            ['get', 'confidence'],
            'confirmed_black', CONFIRMED_COLOR,
            UNVERIFIED_COLOR
          ],
          'circle-stroke-width': 1,
          'circle-stroke-color': '#ffffff',
          'circle-opacity': 0.85
        }
      });

      // ── Dot click → popup ────────────────────────────────────────────────
      map.on('click', 'businesses-circles', function (e) {
        var props  = e.features[0].properties;
        var coords = e.features[0].geometry.coordinates.slice();

        if (activePopup) activePopup.remove();

        var desc = props.description
          ? (props.description.length > 120 ? props.description.substring(0,120) + '…' : props.description)
          : '';
        var href = safeUrl(props.website || '');
        var websiteHtml = href
          ? '<div class="popup-website"><a href="' + escHtml(href) +
            '" target="_blank" rel="noopener">' +
            escHtml(href.replace(/^https?:\/\//,'').split('/')[0]) + ' ↗</a></div>'
          : '';

        var popupHtml =
          '<div class="popup-name">'  + escHtml(props.business_name || '') + '</div>' +
          '<div class="popup-conf">'  + escHtml(confidenceLabel(props.confidence)) + '</div>' +
          '<div class="popup-meta">'  + escHtml(props.industry || '') + ' · ' + escHtml(props.address_city || '') + '</div>' +
          (props.owner_name   ? '<div class="popup-field"><span class="popup-label">Owner: </span>'   + escHtml(props.owner_name)   + '</div>' : '') +
          (props.year_founded ? '<div class="popup-field"><span class="popup-label">Founded: </span>' + escHtml(props.year_founded) + '</div>' : '') +
          (desc ? '<div class="popup-field"><span class="popup-label">About: </span>' + escHtml(desc) + '</div>' : '') +
          websiteHtml;

        activePopup = new mapboxgl.Popup({ maxWidth: '280px', closeButton: true })
          .setLngLat(coords)
          .setHTML(popupHtml)
          .addTo(map);
      });

      map.on('mouseenter', 'businesses-circles', function () {
        map.getCanvas().style.cursor = 'pointer';
      });
      map.on('mouseleave', 'businesses-circles', function () {
        map.getCanvas().style.cursor = '';
      });

      // ── State hover tooltip ──────────────────────────────────────────────
      stateHoverPopup = new mapboxgl.Popup({
        closeButton: false,
        closeOnClick: false,
        className: 'state-hover-popup'
      });

      map.on('mousemove', 'states-fill', function (e) {
        if (!e.features.length) return;
        var feature   = e.features[0];
        var stateName = feature.properties.name;
        var fid       = feature.id;

        if (hoveredStateId !== null && hoveredStateId !== fid) {
          map.setFeatureState({ source: 'us-states', id: hoveredStateId }, { hovered: false });
        }
        hoveredStateId = fid;
        map.setFeatureState({ source: 'us-states', id: fid }, { hovered: true });

        map.getCanvas().style.cursor = 'pointer';

        var count = sourceCounts[stateName] || 0;
        var label = coveredStates[stateName]
          ? escHtml(stateName) + ' &mdash; <strong>' + count.toLocaleString() + '</strong> business' + (count !== 1 ? 'es' : '') + ' in database'
          : escHtml(stateName) + ' &mdash; Not yet covered';

        stateHoverPopup
          .setLngLat(e.lngLat)
          .setHTML('<div class="state-popup-inner">' + label + '</div>')
          .addTo(map);
      });

      map.on('mouseleave', 'states-fill', function () {
        if (hoveredStateId !== null) {
          map.setFeatureState({ source: 'us-states', id: hoveredStateId }, { hovered: false });
          hoveredStateId = null;
        }
        map.getCanvas().style.cursor = '';
        stateHoverPopup.remove();
      });

      // ── State click → filter ─────────────────────────────────────────────
      map.on('click', 'states-fill', function (e) {
        if (!e.features.length) return;
        var stateName = e.features[0].properties.name;
        if (!coveredStates[stateName]) return;
        var sel = document.getElementById('filter-state');
        if (sel) {
          sel.value = stateName;
          sel.dispatchEvent(new Event('change'));
        }
      });

      if (onReady) onReady();
    });
  }

  // ── Apply covered-state feature-states to the map ─────────────────────────
  function applyCoverageToMap() {
    var features = map.querySourceFeatures('us-states');
    var seen = {};
    features.forEach(function (f) {
      if (seen[f.id]) return;
      seen[f.id] = true;
      map.setFeatureState(
        { source: 'us-states', id: f.id },
        { covered: !!coveredStates[f.properties.name] }
      );
    });
  }

  // ── Build GeoJSON FeatureCollection from rows ─────────────────────────────
  function buildGeoJSON(rows) {
    var features = [];
    rows.forEach(function (row) {
      var lat = parseFloat(row.latitude);
      var lon = parseFloat(row.longitude);
      if (isNaN(lat) || isNaN(lon)) return;
      features.push({
        type: 'Feature',
        geometry: { type: 'Point', coordinates: [lon, lat] },
        properties: {
          business_name: row.business_name || '',
          owner_name:    row.owner_name    || '',
          year_founded:  row.year_founded  || '',
          address_city:  row.address_city  || '',
          industry:      row.industry      || '',
          website:       row.website       || '',
          description:   row.description   || '',
          confidence:    row.confidence    || ''
        }
      });
    });
    return { type: 'FeatureCollection', features: features };
  }

  // ── Update business dots layer ─────────────────────────────────────────────
  function updateDots(rows) {
    var src = map.getSource('businesses');
    if (src) src.setData(buildGeoJSON(rows));
  }

  // ── Recenter map ───────────────────────────────────────────────────────────
  function recenterMap(stateName) {
    if (!stateName) {
      map.easeTo({ center: US_CENTER, zoom: US_ZOOM, duration: 600 });
      return;
    }
    var features = map.querySourceFeatures('us-states');
    for (var i = 0; i < features.length; i++) {
      if (features[i].properties.name === stateName) {
        var coords = flattenCoords(features[i].geometry);
        if (coords.length > 0) {
          var bounds = coords.reduce(function (b, c) {
            return b.extend(c);
          }, new mapboxgl.LngLatBounds(coords[0], coords[0]));
          map.fitBounds(bounds, { padding: 60, maxZoom: 7, duration: 600 });
          return;
        }
      }
    }
    map.easeTo({ center: US_CENTER, zoom: US_ZOOM, duration: 600 });
  }

  function flattenCoords(geometry) {
    var out = [];
    function walk(coords) {
      if (typeof coords[0] === 'number') {
        out.push(coords);
      } else {
        coords.forEach(walk);
      }
    }
    walk(geometry.coordinates);
    return out;
  }

  // ── Table ──────────────────────────────────────────────────────────────────
  var EXPANDED_COLS = [7, 8, 9, 10, 11];
  function initTable(rows) {
    table = $('#business-table').DataTable({
      data: rows, pageLength: 25, lengthChange: false, autoWidth: false, dom: 'tip',
      columnDefs: [{ targets: EXPANDED_COLS, visible: false }],
      columns: [
        { data: 'business_name', title: 'Business Name' },
        { data: 'owner_name',    title: 'Owner',    defaultContent: '—' },
        { data: 'address_city',  title: 'City',     defaultContent: '—' },
        { data: 'address_state', title: 'State',    defaultContent: '—' },
        { data: 'industry',      title: 'Industry', defaultContent: '—' },
        { data: 'year_founded',  title: 'Founded',  defaultContent: '—' },
        { data: 'website', title: 'Website', defaultContent: '—', orderable: false,
          render: function (data) {
            var href = safeUrl(data); if (!href) return '—';
            return '<a href="' + escHtml(href) + '" target="_blank" rel="noopener">' +
              escHtml(href.replace(/^https?:\/\//,'').split('/')[0]) + ' ↗</a>';
          } },
        // Expanded columns (hidden by default)
        { data: 'address_street', title: 'Address',       defaultContent: '—' },
        { data: 'certification',  title: 'Certification', defaultContent: '—' },
        { data: 'confidence',     title: 'Confidence',    orderable: false,
          render: function (data) { return confidenceBadge(data); } },
        { data: 'naics_code',     title: 'NAICS',         defaultContent: '—' },
        { data: 'description',    title: 'Description',   defaultContent: '—',
          render: function (data) { return !data ? '—' : escHtml(data.length > 150 ? data.substring(0,150) + '…' : data); } }
      ]
    });
  }

  // ── Filters ────────────────────────────────────────────────────────────────
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
           (row.address_city  || '').toLowerCase().indexOf(q) !== -1 ||
           (row.industry      || '').toLowerCase().indexOf(q) !== -1;
  }

  function applyFilters() {
    var state = (document.getElementById('filter-state') || {}).value || '';
    var city  = (document.getElementById('filter-city')  || {}).value || '';
    var q     = ((document.getElementById('table-search') || {}).value || '').toLowerCase();

    var filtered = allRows.filter(function (r) {
      if (state && r.sourceState !== state) return false;
      if (city  && r.address_city !== city)  return false;
      return matchesText(r, q);
    });

    // Update dots via GL source
    updateDots(filtered);

    // Update table
    if (table) { table.clear(); table.rows.add(filtered); table.draw(); }

    // Update result count
    var countEl = document.getElementById('result-count');
    if (countEl) countEl.textContent = filtered.length.toLocaleString() + ' shown';

    // Recenter
    recenterMap(state || null);
  }

  // ── Coverage bar ──────────────────────────────────────────────────────────
  var US_BLACK_EMPLOYER_BUSINESSES = 160000;
  function updateCoverageBar(count) {
    var pct    = count / US_BLACK_EMPLOYER_BUSINESSES * 100;
    var fillEl = document.getElementById('coverage-fill');
    var pctEl  = document.getElementById('coverage-pct');
    if (fillEl) fillEl.style.width = Math.min(pct, 100) + '%';
    if (pctEl)  pctEl.textContent  = pct.toFixed(1) + '%';
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

        // Stat counts
        var countEl = document.getElementById('stat-count');
        if (countEl) countEl.textContent = allRows.length.toLocaleString();
        var n = Object.keys(coveredStates).length;
        var coverageEl = document.getElementById('stat-coverage');
        if (coverageEl) coverageEl.textContent = n + ' State' + (n !== 1 ? 's' : '');

        updateCoverageBar(allRows.length);

        // Push dots and coverage to map.
        // Strategy: if the map style is already loaded push immediately,
        // then re-apply coverage on the next idle event (tiles in cache).
        function pushToMap() {
          updateDots(allRows);
          // applyCoverageToMap needs tiles rendered; use once('idle') to be safe.
          function tryApplyCoverage() {
            var feats = map.querySourceFeatures('us-states');
            if (feats.length > 0) applyCoverageToMap();
            map.once('idle', applyCoverageToMap);
          }
          tryApplyCoverage();
        }

        if (map.isStyleLoaded()) {
          pushToMap();
        } else {
          map.once('load', pushToMap);
        }

        populateStateFilter();
        initTable(allRows);
        var rc = document.getElementById('result-count');
        if (rc) rc.textContent = allRows.length.toLocaleString() + ' shown';
      },
      error: function (err) { console.error('Could not load businesses.csv:', err); }
    });
  }

  // ── Init ──────────────────────────────────────────────────────────────────
  document.addEventListener('DOMContentLoaded', function () {
    initMap(function () {
      // Map is ready — loadData() may have already fired; that's fine,
      // it handles the "map not yet loaded" case internally.
    });
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
    if (stateSel) stateSel.addEventListener('change', function () {
      populateCityFilter(this.value);
      applyFilters();
    });
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
