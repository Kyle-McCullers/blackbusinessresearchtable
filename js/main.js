(function () {
  'use strict';

  // ── Mapbox token ──────────────────────────────────────────────────────────
  // Public token, restricted by URL in the Mapbox account (Tokens → Allowed URLs).
  var MAPBOX_TOKEN = 'pk.eyJ1Ijoia3lsZW1jY3VsbGVycyIsImEiOiJjbXFlMmY2ZGMxNnc0MnJvZ2k0bnE0aWV5In0.6MIiC0eq67WYPLesyliFAQ';
  mapboxgl.accessToken = MAPBOX_TOKEN;

  // ── Reference tables ──────────────────────────────────────────────────────
  var STATE_NAMES = {
    AL:'Alabama',AK:'Alaska',AZ:'Arizona',AR:'Arkansas',CA:'California',CO:'Colorado',
    CT:'Connecticut',DE:'Delaware',DC:'District of Columbia',FL:'Florida',GA:'Georgia',
    HI:'Hawaii',ID:'Idaho',IL:'Illinois',IN:'Indiana',IA:'Iowa',KS:'Kansas',KY:'Kentucky',
    LA:'Louisiana',ME:'Maine',MD:'Maryland',MA:'Massachusetts',MI:'Michigan',MN:'Minnesota',
    MS:'Mississippi',MO:'Missouri',MT:'Montana',NE:'Nebraska',NV:'Nevada',NH:'New Hampshire',
    NJ:'New Jersey',NM:'New Mexico',NY:'New York',NC:'North Carolina',ND:'North Dakota',
    OH:'Ohio',OK:'Oklahoma',OR:'Oregon',PA:'Pennsylvania',RI:'Rhode Island',SC:'South Carolina',
    SD:'South Dakota',TN:'Tennessee',TX:'Texas',UT:'Utah',VT:'Vermont',VA:'Virginia',
    WA:'Washington',WV:'West Virginia',WI:'Wisconsin',WY:'Wyoming',PR:'Puerto Rico'
  };
  var NAME_TO_ABBR = {};
  Object.keys(STATE_NAMES).forEach(function (k) { NAME_TO_ABBR[STATE_NAMES[k]] = k; });
  var FIPS_STATE = {
    '01':'AL','02':'AK','04':'AZ','05':'AR','06':'CA','08':'CO','09':'CT','10':'DE','11':'DC',
    '12':'FL','13':'GA','15':'HI','16':'ID','17':'IL','18':'IN','19':'IA','20':'KS','21':'KY',
    '22':'LA','23':'ME','24':'MD','25':'MA','26':'MI','27':'MN','28':'MS','29':'MO','30':'MT',
    '31':'NE','32':'NV','33':'NH','34':'NJ','35':'NM','36':'NY','37':'NC','38':'ND','39':'OH',
    '40':'OK','41':'OR','42':'PA','44':'RI','45':'SC','46':'SD','47':'TN','48':'TX','49':'UT',
    '50':'VT','51':'VA','53':'WA','54':'WV','55':'WI','56':'WY','72':'PR'
  };
  var SCOPE_LABEL = { statewide: 'Statewide program', transportation: 'Transportation (DBE) directory', city: 'City program', federal: 'Federal program' };
  var RAMP = ['#e6f3ec', '#c4e3d3', '#98cdb3', '#68b08f', '#3f9070', '#237050', '#134a33'];
  var COLOR_NONE = '#f1f4f2', COLOR_NOSRC = '#e3e3e3';

  // ── State ─────────────────────────────────────────────────────────────────
  var map, mapReady = false, dataReady = false;
  var table = null;
  var rows = [];                 // all businesses
  var sources = [];              // sources.csv rows
  var meta = {};
  var counties = {};             // fips -> {name, st, bbox, polys, cx, cy}
  var countyList = [];
  var grid = {};
  var coverage = {};             // state abbr -> 'statewide' | 'transportation' | 'city'
  var scopeByName = {};          // data_source label -> scope
  var statesGeo = null;
  var filters = { status: 'headline', scope: '', st: '', q: '', fips: '' };
  var layerMode = 'counties';    // 'counties' | 'dots'
  var scaleMode = 'national';    // 'national' | 'screen'
  var current = [];              // rows passing filters
  var countsByFips = {};
  var unmapped = 0;
  var selectedFips = '';
  var hoverTip = null, activePopup = null;

  // ── Utilities ─────────────────────────────────────────────────────────────
  function $(id) { return document.getElementById(id); }
  function escHtml(s) {
    return String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }
  function safeUrl(url) { return /^https?:\/\//i.test(url || '') ? url : ''; }
  function fmt(n) { return Number(n).toLocaleString('en-US'); }
  function fmtDate(iso) {
    if (!iso) return '';
    var d = new Date(iso + 'T12:00:00');
    return isNaN(d) ? iso : d.toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' });
  }
  function setText(id, text) { var el = $(id); if (el) el.textContent = text; }
  function fetchText(url) {
    return fetch(url).then(function (r) { if (!r.ok) throw new Error(url + ' ' + r.status); return r.text(); });
  }
  function parseCsv(text) {
    return Papa.parse(text, { header: true, skipEmptyLines: true }).data;
  }

  // ── County geometry: bounding boxes, grid index, point-in-polygon ─────────
  function prepareCounties(topo) {
    var feats = topojson.feature(topo, topo.objects.counties).features;
    feats.forEach(function (f) {
      var fips = String(f.id).padStart(5, '0');
      f.properties = { fips: fips, name: f.properties.name };
      var polys = f.geometry.type === 'Polygon' ? [f.geometry.coordinates] : f.geometry.coordinates;
      var minx = 180, miny = 90, maxx = -180, maxy = -90;
      polys.forEach(function (p) { p[0].forEach(function (c) {
        if (c[0] < minx) minx = c[0]; if (c[0] > maxx) maxx = c[0];
        if (c[1] < miny) miny = c[1]; if (c[1] > maxy) maxy = c[1];
      }); });
      var c = {
        fips: fips, name: f.properties.name, st: FIPS_STATE[fips.slice(0, 2)] || '',
        bbox: [minx, miny, maxx, maxy], polys: polys, cx: (minx + maxx) / 2, cy: (miny + maxy) / 2
      };
      counties[fips] = c; countyList.push(c);
      for (var gx = Math.floor(minx); gx <= Math.floor(maxx); gx++) {
        for (var gy = Math.floor(miny); gy <= Math.floor(maxy); gy++) {
          var key = gx + ',' + gy;
          (grid[key] = grid[key] || []).push(c);
        }
      }
    });
    return { type: 'FeatureCollection', features: feats };
  }

  function inRing(x, y, ring) {
    var inside = false;
    for (var i = 0, j = ring.length - 1; i < ring.length; j = i++) {
      var xi = ring[i][0], yi = ring[i][1], xj = ring[j][0], yj = ring[j][1];
      if (((yi > y) !== (yj > y)) && (x < (xj - xi) * (y - yi) / (yj - yi) + xi)) inside = !inside;
    }
    return inside;
  }
  function inCounty(x, y, c) {
    var b = c.bbox;
    if (x < b[0] || x > b[2] || y < b[1] || y > b[3]) return false;
    for (var p = 0; p < c.polys.length; p++) {
      var poly = c.polys[p];
      if (!inRing(x, y, poly[0])) continue;
      var hole = false;
      for (var h = 1; h < poly.length; h++) { if (inRing(x, y, poly[h])) { hole = true; break; } }
      if (!hole) return true;
    }
    return false;
  }
  function locateCounty(lon, lat) {
    var gx = Math.floor(lon), gy = Math.floor(lat), list = grid[gx + ',' + gy] || [], i;
    for (i = 0; i < list.length; i++) if (inCounty(lon, lat, list[i])) return list[i].fips;
    // Simplified coastlines can leave a coastal point just outside; take the nearest
    // centroid within roughly half a degree.
    var best = null, bestD = 0.36;
    for (var dx = -1; dx <= 1; dx++) for (var dy = -1; dy <= 1; dy++) {
      (grid[(gx + dx) + ',' + (gy + dy)] || []).forEach(function (c) {
        var d = (c.cx - lon) * (c.cx - lon) + (c.cy - lat) * (c.cy - lat);
        if (d < bestD) { bestD = d; best = c.fips; }
      });
    }
    return best || '';
  }

  // ── Row preparation ───────────────────────────────────────────────────────
  function prepareRows() {
    rows.forEach(function (r) {
      var lat = parseFloat(r.latitude), lon = parseFloat(r.longitude);
      r._lat = isNaN(lat) ? null : lat; r._lon = isNaN(lon) ? null : lon;
      r._fips = (r._lat !== null && r._lon !== null) ? locateCounty(r._lon, r._lat) : '';
      var st = (r.address_state || '').toUpperCase();
      if (!STATE_NAMES[st]) st = r._fips ? (counties[r._fips] || {}).st || '' : '';
      r._st = st;
      r._former = r.certification_status === 'former';
      r._scope = scopeByName[r.data_source] || '';
      r._hay = ((r.business_name || '') + ' ' + (r.address_city || '') + ' ' + (r.industry || '')).toLowerCase();
    });
  }

  // ── Coverage (defined by programs, not by where listed firms sit) ─────────
  function computeCoverage() {
    var rank = { statewide: 3, transportation: 2, city: 1 };
    sources.forEach(function (s) {
      s.records = parseInt(s.records, 10) || 0;
      s.retained_records = parseInt(s.retained_records, 10) || 0;
      scopeByName[s.source_name] = s.scope;
      if (s.records > 0 && s.state && rank[s.scope]) {
        if (!coverage[s.state] || rank[s.scope] > rank[coverage[s.state]]) coverage[s.state] = s.scope;
      }
    });
  }

  function renderCoverageText() {
    var withData = sources.filter(function (s) { return s.records > 0; });
    var by = { statewide: [], transportation: [], city: [] };
    Object.keys(coverage).forEach(function (st) { by[coverage[st]].push(st); });
    var total = Object.keys(coverage).length;
    var hasDC = !!coverage.DC;
    var stateCount = total - (hasDC ? 1 : 0);
    var firmStates = {};
    rows.forEach(function (r) { if (r._st && r._st !== 'DC') firmStates[r._st] = 1; });
    var nFirmStates = Object.keys(firmStates).length;
    var noProgram = 50 - stateCount;
    var headline = rows.filter(function (r) { return !r._former; }).length;
    var former = rows.length - headline;

    setText('stat-count', fmt(headline));
    setText('stat-programs', fmt(withData.length));
    setText('stat-jurisdictions', fmt(total));
    var asOf = meta.snapshot_id || '—';
    setText('stat-asof', asOf);
    setText('cite-snapshot', asOf);
    setText('footer-asof', 'Snapshot ' + asOf + (meta.run_date ? ' · refreshed ' + fmtDate(meta.run_date) : ''));

    $('stat-footnote').textContent =
      'The Businesses figure counts listings that are currently certified, plus listings from programs that do not report certification status. ' +
      (former ? 'A further ' + fmt(former) + ' firms were certified in the past and are kept in the data (' + 'certification_status = former); including them gives ' + fmt(rows.length) + '. ' : '') +
      'Certification records show whether a program lists a firm, not whether the business exists or operates. These are listings, not yet deduplicated firms.';

    $('coverage-lede').textContent =
      'Records come from ' + withData.length + ' public programs in ' + total + ' jurisdictions (' + stateCount + ' states' + (hasDC ? ' and DC' : '') + '): ' +
      by.statewide.length + ' with a statewide program, ' + by.transportation.length + ' with a transportation (DBE) directory only, and ' +
      by.city.length + ' with a city program only.';
    $('coverage-defn').textContent =
      'Coverage means a jurisdiction has a program we collect from. Listed firms are based in ' + nFirmStates + ' states, because firms often certify outside their home state, ' +
      'but ' + noProgram + ' states have no program here. Do not read the number of states that contain a listed firm as the number of states covered, and do not read a pale county as few firms existing.';
  }

  function renderSourcesTable() {
    var tb = document.querySelector('#sources-table tbody');
    if (!tb) return;
    var order = ['statewide', 'transportation', 'city', 'federal'];
    var html = '';
    order.forEach(function (scope) {
      var list = sources.filter(function (s) { return s.scope === scope; })
        .sort(function (a, b) { return (a.state || 'ZZ').localeCompare(b.state || 'ZZ') || a.source_name.localeCompare(b.source_name); });
      if (!list.length) return;
      html += '<tr class="grp"><td colspan="5">' + escHtml(SCOPE_LABEL[scope]) + 's</td></tr>';
      list.forEach(function (s) {
        var covers = s.place ? s.place + ', ' + s.state : (STATE_NAMES[s.state] || 'National');
        var last;
        if (!s.records) last = '<span class="tag-stale">not loaded</span>';
        else if (s.retained_records >= s.records) last = escHtml(fmtDate(s.last_observed)) + ' <span class="tag-stale">carried forward</span>';
        else last = escHtml(fmtDate(s.last_observed));
        html += '<tr><td>' + escHtml(s.source_name) + '</td><td>' + escHtml(covers) + '</td><td>' + escHtml(s.program) +
          '</td><td class="num">' + fmt(s.records) + '</td><td>' + last + '</td></tr>';
      });
    });
    tb.innerHTML = html;
  }

  // ── Filtering ─────────────────────────────────────────────────────────────
  function passes(r, ignoreFips) {
    if (filters.status === 'headline' && r._former) return false;
    if (filters.status === 'former' && !r._former) return false;
    if (filters.scope && r._scope !== filters.scope) return false;
    if (filters.st && r._st !== filters.st) return false;
    if (!ignoreFips && filters.fips && r._fips !== filters.fips) return false;
    if (filters.q && r._hay.indexOf(filters.q) === -1) return false;
    return true;
  }

  function applyFilters(opts) {
    opts = opts || {};
    current = rows.filter(function (r) { return passes(r, false); });
    countsByFips = {}; unmapped = 0;
    var forMap = filters.fips ? rows.filter(function (r) { return passes(r, true); }) : current;
    forMap.forEach(function (r) {
      if (r._fips) countsByFips[r._fips] = (countsByFips[r._fips] || 0) + 1; else unmapped++;
    });

    setText('result-n', fmt(current.length));
    setText('result-l', filters.status === 'former' ? 'formerly certified listings' : 'listings match');
    var sub = '';
    if (filters.fips && counties[filters.fips]) {
      sub = 'Limited to ' + counties[filters.fips].name + ', ' + counties[filters.fips].st + ' — <a href="#" id="clear-fips">show all</a>. ';
    }
    var noLoc = current.filter(function (r) { return !r._fips; }).length;
    if (noLoc) sub += fmt(noLoc) + ' have no map location and appear only in the table.';
    $('result-sub').innerHTML = sub;
    var clr = $('clear-fips');
    if (clr) clr.addEventListener('click', function (e) { e.preventDefault(); filters.fips = ''; applyFilters(); });

    setText('result-count', fmt(current.length) + ' shown');
    if (table) { table.clear(); table.rows.add(current); table.draw(); }
    if (mapReady) { updateChoropleth(); updatePoints(); }
    if (opts.fitState) fitToState(filters.st);
  }

  // ── Choropleth ────────────────────────────────────────────────────────────
  function quantileBreaks(values) {
    if (!values.length) return [1];
    values.sort(function (a, b) { return a - b; });
    var out = [], n = values.length, k = RAMP.length;
    for (var i = 1; i <= k; i++) {
      var v = values[Math.min(n - 1, Math.ceil(i / k * n) - 1)];
      if (!out.length || v > out[out.length - 1]) out.push(v);
    }
    return out;
  }
  function visibleCounties() {
    var b = map.getBounds(), w = b.getWest(), e = b.getEast(), s = b.getSouth(), n = b.getNorth();
    return countyList.filter(function (c) { return !(c.bbox[2] < w || c.bbox[0] > e || c.bbox[3] < s || c.bbox[1] > n); });
  }
  var lastBreaks = [1];
  function updateChoropleth() {
    var pool = scaleMode === 'screen' ? visibleCounties() : countyList;
    var vals = [];
    pool.forEach(function (c) { var n = countsByFips[c.fips] || 0; if (n > 0) vals.push(n); });
    var breaks = quantileBreaks(vals);
    lastBreaks = breaks;
    countyList.forEach(function (c) {
      var n = countsByFips[c.fips] || 0, cls;
      if (n > 0) {
        cls = 1; while (cls < breaks.length && n > breaks[cls - 1]) cls++;
      } else {
        cls = coverage[c.st] ? 0 : -1;
      }
      map.setFeatureState({ source: 'counties', id: c.fips }, { cls: cls, n: n });
    });
    renderLegend(breaks);
  }
  function abbr(n) { return n >= 10000 ? Math.round(n / 1000) + 'k' : n >= 1000 ? (Math.round(n / 100) / 10) + 'k' : String(n); }
  function renderLegend(breaks) {
    var html = '';
    breaks.forEach(function (hi, i) {
      var lo = i === 0 ? 1 : breaks[i - 1] + 1;
      var label = lo >= hi ? abbr(hi) : abbr(lo) + '–' + abbr(hi);
      html += '<div class="seg-c"><i style="background:' + RAMP[i] + '"></i><span>' + label + '</span></div>';
    });
    $('legend-ramp').innerHTML = html;
    setText('legend-title', filters.status === 'former' ? 'Formerly certified listings per county' : 'Listings per county');
  }
  function fillColorExpr() {
    var expr = ['match', ['coalesce', ['feature-state', 'cls'], 0], -1, COLOR_NOSRC, 0, COLOR_NONE];
    RAMP.forEach(function (c, i) { expr.push(i + 1, c); });
    expr.push(COLOR_NONE);
    return expr;
  }

  // ── Points (clustered) ────────────────────────────────────────────────────
  function updatePoints() {
    var src = map.getSource('points');
    if (!src) return;
    var feats = [];
    current.forEach(function (r) {
      if (r._lat === null) return;
      feats.push({ type: 'Feature', geometry: { type: 'Point', coordinates: [r._lon, r._lat] }, properties: {
        name: r.business_name || '', city: r.address_city || '', st: r._st, industry: r.industry || '',
        website: r.website || '', year: r.year_founded || '', former: r._former ? 1 : 0,
        source: r.data_source || '', desc: (r.description || '').substring(0, 140)
      } });
    });
    src.setData({ type: 'FeatureCollection', features: feats });
  }

  function setLayerMode(mode) {
    layerMode = mode;
    $('layer-counties').classList.toggle('active', mode === 'counties');
    $('layer-dots').classList.toggle('active', mode === 'dots');
    if (!mapReady) return;
    var dots = mode === 'dots';
    ['clusters', 'cluster-count', 'unclustered'].forEach(function (id) {
      map.setLayoutProperty(id, 'visibility', dots ? 'visible' : 'none');
    });
    map.setPaintProperty('counties-fill', 'fill-opacity', dots ? 0.22 : 0.92);
    $('map-legend').classList.toggle('dots-mode', dots);
    $('legend-ramp').style.display = dots ? 'none' : 'flex';
    setText('legend-title', dots ? 'Individual listings (green = current, gold = former)' : 'Listings per county');
    if (!dots) renderLegend(lastBreaks);
  }

  // ── Map ───────────────────────────────────────────────────────────────────
  function initMap(countiesGeo) {
    map = new mapboxgl.Map({
      container: 'map', style: 'mapbox://styles/mapbox/light-v11',
      center: [-96, 37.6], zoom: 3.55, minZoom: 2.5, attributionControl: true, preserveDrawingBuffer: true
    });
    map.addControl(new mapboxgl.NavigationControl({ showCompass: false }), 'top-right');
    map.on('load', function () {
      var firstLabel;
      (map.getStyle().layers || []).some(function (l) { if (l.type === 'symbol') { firstLabel = l.id; return true; } return false; });

      map.addSource('counties', { type: 'geojson', data: countiesGeo, promoteId: 'fips' });
      map.addLayer({ id: 'counties-fill', type: 'fill', source: 'counties',
        paint: { 'fill-color': fillColorExpr(), 'fill-opacity': 0.92 } }, firstLabel);
      map.addLayer({ id: 'counties-line', type: 'line', source: 'counties',
        paint: { 'line-color': '#ffffff', 'line-width': ['interpolate', ['linear'], ['zoom'], 3, 0.15, 8, 0.8], 'line-opacity': 0.8 } }, firstLabel);
      map.addLayer({ id: 'counties-selected', type: 'line', source: 'counties', filter: ['==', ['get', 'fips'], ''],
        paint: { 'line-color': '#c9951a', 'line-width': 3 } }, firstLabel);

      statesGeo.features.forEach(function (f) { f.properties.cov = coverage[NAME_TO_ABBR[f.properties.name]] || 'none'; });
      map.addSource('states', { type: 'geojson', data: statesGeo });
      map.addLayer({ id: 'states-statewide', type: 'line', source: 'states', filter: ['==', ['get', 'cov'], 'statewide'],
        paint: { 'line-color': '#1B4332', 'line-width': 2 } }, firstLabel);
      map.addLayer({ id: 'states-transportation', type: 'line', source: 'states', filter: ['==', ['get', 'cov'], 'transportation'],
        paint: { 'line-color': '#1B4332', 'line-width': 2, 'line-dasharray': [3, 2] } }, firstLabel);
      map.addLayer({ id: 'states-city', type: 'line', source: 'states', filter: ['==', ['get', 'cov'], 'city'],
        paint: { 'line-color': '#1B4332', 'line-width': 2, 'line-dasharray': [0.4, 2.2] }, layout: { 'line-cap': 'round' } }, firstLabel);
      map.addLayer({ id: 'states-none', type: 'line', source: 'states', filter: ['==', ['get', 'cov'], 'none'],
        paint: { 'line-color': '#9aa5a0', 'line-width': 0.8 } }, firstLabel);

      map.addSource('points', { type: 'geojson', data: { type: 'FeatureCollection', features: [] },
        cluster: true, clusterRadius: 42, clusterMaxZoom: 10 });
      map.addLayer({ id: 'clusters', type: 'circle', source: 'points', filter: ['has', 'point_count'],
        layout: { visibility: 'none' },
        paint: { 'circle-color': ['step', ['get', 'point_count'], '#52a77e', 50, '#2D6A4F', 500, '#1B4332'],
          'circle-radius': ['step', ['get', 'point_count'], 14, 50, 19, 500, 26],
          'circle-stroke-width': 2, 'circle-stroke-color': '#fff' } });
      map.addLayer({ id: 'cluster-count', type: 'symbol', source: 'points', filter: ['has', 'point_count'],
        layout: { visibility: 'none', 'text-field': ['get', 'point_count_abbreviated'], 'text-size': 12,
          'text-font': ['DIN Pro Medium', 'Arial Unicode MS Bold'] }, paint: { 'text-color': '#ffffff' } });
      map.addLayer({ id: 'unclustered', type: 'circle', source: 'points', filter: ['!', ['has', 'point_count']],
        layout: { visibility: 'none' },
        paint: { 'circle-radius': 5.5, 'circle-color': ['case', ['==', ['get', 'former'], 1], '#c9951a', '#1B4332'],
          'circle-stroke-width': 1.5, 'circle-stroke-color': '#fff' } });

      hoverTip = new mapboxgl.Popup({ closeButton: false, closeOnClick: false, className: 'map-tip-pop', offset: 12 });
      map.on('mousemove', 'counties-fill', function (e) {
        if (layerMode === 'dots' && map.queryRenderedFeatures(e.point, { layers: ['clusters', 'unclustered'] }).length) return;
        var f = e.features[0]; if (!f) return;
        var c = counties[f.properties.fips]; if (!c) return;
        var n = countsByFips[c.fips] || 0;
        var tail = n ? fmt(n) + ' listed' : (coverage[c.st] ? 'none listed' : 'no program in this state yet');
        hoverTip.setLngLat(e.lngLat).setHTML('<div class="map-tip"><b>' + escHtml(c.name) + ', ' + c.st + '</b> — ' + tail + '</div>').addTo(map);
        map.getCanvas().style.cursor = 'pointer';
      });
      map.on('mouseleave', 'counties-fill', function () { hoverTip.remove(); map.getCanvas().style.cursor = ''; });
      map.on('click', 'counties-fill', function (e) {
        if (layerMode === 'dots' && map.queryRenderedFeatures(e.point, { layers: ['clusters', 'unclustered'] }).length) return;
        var f = e.features[0]; if (f) selectCounty(f.properties.fips);
      });
      map.on('click', 'clusters', function (e) {
        var f = map.queryRenderedFeatures(e.point, { layers: ['clusters'] })[0];
        map.getSource('points').getClusterExpansionZoom(f.properties.cluster_id, function (err, z) {
          if (!err) map.easeTo({ center: f.geometry.coordinates, zoom: z + 0.2 });
        });
      });
      map.on('click', 'unclustered', function (e) { showFirm(e.features[0]); });
      ['clusters', 'unclustered'].forEach(function (id) {
        map.on('mouseenter', id, function () { map.getCanvas().style.cursor = 'pointer'; });
        map.on('mouseleave', id, function () { map.getCanvas().style.cursor = ''; });
      });

      var t = null;
      map.on('moveend', function () {
        if (scaleMode !== 'screen' || layerMode !== 'counties') return;
        clearTimeout(t); t = setTimeout(updateChoropleth, 150);
      });

      mapReady = true;
      setLayerMode(layerMode);
      applyFilters();
      var ld = $('map-loading'); if (ld) ld.hidden = true;
      var qa = /[?&]qa=(\w+)/.exec(location.search);   // screenshot/QA hook: jump to a section
      if (qa && $(qa[1])) { document.documentElement.style.scrollBehavior = 'auto'; window.scrollTo(0, $(qa[1]).offsetTop - 64); }
    });
  }

  function showFirm(f) {
    var p = f.properties, coords = f.geometry.coordinates.slice();
    if (activePopup) activePopup.remove();
    var href = safeUrl(p.website);
    var html = '<div class="popup-name">' + escHtml(p.name) + '</div>' +
      '<div class="popup-meta">' + escHtml([p.industry, p.city && (p.city + (p.st ? ', ' + p.st : ''))].filter(Boolean).join(' · ')) + '</div>' +
      '<span class="popup-pill' + (Number(p.former) ? ' former' : '') + '">' + (Number(p.former) ? 'Formerly certified' : 'Listed') + '</span>' +
      (p.year ? '<div class="popup-field"><b>Founded:</b> ' + escHtml(p.year) + '</div>' : '') +
      (p.desc ? '<div class="popup-field">' + escHtml(p.desc) + '</div>' : '') +
      '<div class="popup-field"><b>Source:</b> ' + escHtml(p.source) + '</div>' +
      (href ? '<div class="popup-field"><a href="' + escHtml(href) + '" target="_blank" rel="noopener">' + escHtml(href.replace(/^https?:\/\//, '').split('/')[0]) + ' ↗</a></div>' : '');
    activePopup = new mapboxgl.Popup({ maxWidth: '300px' }).setLngLat(coords).setHTML(html).addTo(map);
  }

  function selectCounty(fips) {
    var c = counties[fips]; if (!c) return;
    selectedFips = fips;
    map.setFilter('counties-selected', ['==', ['get', 'fips'], fips]);
    var inCounty = rows.filter(function (r) { return r._fips === fips && passes(r, true); });
    var former = inCounty.filter(function (r) { return r._former; }).length;
    var ind = {}, src = {};
    inCounty.forEach(function (r) {
      if (r.industry) { var k = r.industry.split(/[;,]/)[0].trim().substring(0, 48); if (k) ind[k] = (ind[k] || 0) + 1; }
      if (r.data_source) src[r.data_source] = (src[r.data_source] || 0) + 1;
    });
    function top(o, n) { return Object.keys(o).sort(function (a, b) { return o[b] - o[a]; }).slice(0, n); }
    var html = '<button class="sel-x" id="sel-x" aria-label="Close">×</button>' +
      '<div class="sel-title">' + escHtml(c.name) + ', ' + c.st + '</div>' +
      '<div class="sel-row"><span>Listings matching your filters</span><b>' + fmt(inCounty.length) + '</b></div>' +
      (filters.status === 'all' ? '<div class="sel-row"><span>…of which formerly certified</span><b>' + fmt(former) + '</b></div>' : '') +
      '<div class="sel-row"><span>Program in this state</span><b>' + escHtml(coverage[c.st] ? SCOPE_LABEL[coverage[c.st]].replace(' program', '').replace(' directory', '') : 'None yet') + '</b></div>';
    if (top(ind, 3).length) html += '<ul class="sel-list">' + top(ind, 3).map(function (k) { return '<li>' + escHtml(k) + ' (' + ind[k] + ')</li>'; }).join('') + '</ul>';
    if (top(src, 3).length) html += '<div class="sb-note" style="color:#6b7770;margin-top:8px">Listed by: ' + top(src, 3).map(escHtml).join('; ') + '</div>';
    html += inCounty.length ? '<button class="sel-btn" id="sel-table" type="button">Show these in the table</button>' : '<div class="sb-note" style="color:#6b7770">None listed here does not mean none exist.</div>';
    var panel = $('selected-panel'); panel.innerHTML = html; panel.hidden = false;
    $('sel-x').addEventListener('click', clearSelection);
    var tb = $('sel-table');
    if (tb) tb.addEventListener('click', function () { filters.fips = fips; applyFilters(); $('data').scrollIntoView({ behavior: 'smooth' }); });
  }
  function clearSelection() {
    selectedFips = '';
    if (mapReady) map.setFilter('counties-selected', ['==', ['get', 'fips'], '']);
    $('selected-panel').hidden = true;
  }

  function fitToState(st) {
    if (!mapReady) return;
    if (!st) { map.easeTo({ center: [-96, 37.6], zoom: 3.55, duration: 600 }); return; }
    var b = null;
    countyList.forEach(function (c) {
      if (c.st !== st) return;
      if (!b) b = [c.bbox[0], c.bbox[1], c.bbox[2], c.bbox[3]];
      else { b[0] = Math.min(b[0], c.bbox[0]); b[1] = Math.min(b[1], c.bbox[1]); b[2] = Math.max(b[2], c.bbox[2]); b[3] = Math.max(b[3], c.bbox[3]); }
    });
    if (b && st !== 'AK' && st !== 'HI') map.fitBounds([[b[0], b[1]], [b[2], b[3]]], { padding: 50, duration: 600 });
    else if (b) map.fitBounds([[b[0], b[1]], [b[2], b[3]]], { padding: 50, duration: 600, maxZoom: 6 });
  }

  // ── Place search (Mapbox Geocoding) ───────────────────────────────────────
  function searchPlace(q) {
    var note = $('place-note'); note.textContent = '';
    if (!q) return;
    var url = 'https://api.mapbox.com/geocoding/v5/mapbox.places/' + encodeURIComponent(q) +
      '.json?country=us&limit=1&types=place,postcode,address,district,region&access_token=' + MAPBOX_TOKEN;
    fetch(url).then(function (r) { return r.json(); }).then(function (d) {
      var f = d.features && d.features[0];
      if (!f) { note.textContent = 'No place found. Try a city, ZIP or street address.'; return; }
      if (f.bbox) map.fitBounds([[f.bbox[0], f.bbox[1]], [f.bbox[2], f.bbox[3]]], { padding: 40, maxZoom: 11, duration: 800 });
      else map.flyTo({ center: f.center, zoom: 10, duration: 800 });
      note.textContent = f.place_name;
      note.style.color = '#bfe0cf';
    }).catch(function () { note.textContent = 'Search is unavailable right now.'; });
  }

  // ── Table ─────────────────────────────────────────────────────────────────
  var EXPANDED_COLS = [6, 7, 8, 9, 10, 11];
  function initTable() {
    table = window.jQuery('#business-table').DataTable({
      data: current, pageLength: 25, lengthChange: false, autoWidth: false, dom: 'tip',
      columnDefs: [{ targets: EXPANDED_COLS, visible: false }],
      columns: [
        { data: 'business_name', title: 'Business name' },
        { data: 'address_city', title: 'City', defaultContent: '—' },
        { data: '_st', title: 'State', defaultContent: '—' },
        { data: 'industry', title: 'Industry', defaultContent: '—' },
        { data: 'year_founded', title: 'Founded', defaultContent: '—' },
        { data: 'website', title: 'Website', defaultContent: '—', orderable: false,
          render: function (d) {
            var href = safeUrl(d); if (!href) return '—';
            return '<a href="' + escHtml(href) + '" target="_blank" rel="noopener">' + escHtml(href.replace(/^https?:\/\//, '').split('/')[0]) + ' ↗</a>';
          } },
        { data: 'address_street', title: 'Address', defaultContent: '—' },
        { data: 'certification', title: 'Certification', defaultContent: '—' },
        { data: 'confidence', title: 'Basis', orderable: false,
          render: function (d) { return d === 'confirmed_black' ? '<span class="badge badge-confirmed">Source states Black-owned</span>' : '<span class="badge badge-unverified">MBE, unverified</span>'; } },
        { data: 'naics_code', title: 'NAICS', defaultContent: '—' },
        { data: 'certification_status', title: 'Status', defaultContent: '—',
          render: function (d) { return d === 'current' ? 'Current' : d === 'former' ? 'Former' : '—'; } },
        { data: 'description', title: 'Description', defaultContent: '—',
          render: function (d) { return !d ? '—' : escHtml(d.length > 150 ? d.substring(0, 150) + '…' : d); } }
      ]
    });
  }

  // ── Forms ─────────────────────────────────────────────────────────────────
  function wireForm(formId, successId, label) {
    var form = $(formId); if (!form) return;
    form.addEventListener('submit', function (e) {
      e.preventDefault();
      var btn = form.querySelector('button[type="submit"]');
      if (btn) { btn.disabled = true; btn.textContent = 'Sending…'; }
      fetch(form.action, { method: 'POST', body: new FormData(form), headers: { 'Accept': 'application/json' } })
        .then(function (resp) {
          if (resp.ok) { form.hidden = true; $(successId).hidden = false; }
          else throw new Error('bad status');
        }).catch(function () {
          if (btn) { btn.disabled = false; btn.textContent = label; }
          alert('Something went wrong. Please email data@blackbusinessresearchtable.com.');
        });
    });
  }

  // ── Guide modal ───────────────────────────────────────────────────────────
  function openGuide() { $('guide').hidden = false; $('guide-go').focus(); }
  function closeGuide() {
    $('guide').hidden = true;
    try { localStorage.setItem('bbrt_guide_seen', '1'); } catch (e) { /* storage unavailable */ }
  }
  function maybeShowGuideOnce() {
    var seen = /[?&]noguide\b/.test(location.search);
    try { seen = seen || localStorage.getItem('bbrt_guide_seen') === '1'; } catch (e) { /* ignore */ }
    if (seen || !('IntersectionObserver' in window)) return;
    var io = new IntersectionObserver(function (entries) {
      if (entries[0].isIntersecting) { io.disconnect(); setTimeout(openGuide, 500); }
    }, { threshold: 0.5 });
    io.observe($('explore'));
  }

  // ── Wiring ────────────────────────────────────────────────────────────────
  function populateStateFilter() {
    var sel = $('filter-state');
    Object.keys(STATE_NAMES).sort(function (a, b) { return STATE_NAMES[a].localeCompare(STATE_NAMES[b]); }).forEach(function (k) {
      var n = rows.filter(function (r) { return r._st === k; }).length;
      if (!n) return;
      var o = document.createElement('option'); o.value = k; o.textContent = STATE_NAMES[k] + ' (' + fmt(n) + ')'; sel.appendChild(o);
    });
  }

  function wireControls() {
    $('filter-status').addEventListener('change', function () { filters.status = this.value; applyFilters(); });
    $('filter-scope').addEventListener('change', function () { filters.scope = this.value; applyFilters(); });
    $('filter-state').addEventListener('change', function () { filters.st = this.value; filters.fips = ''; clearSelection(); applyFilters({ fitState: true }); });
    var qt = null;
    $('table-search').addEventListener('input', function () {
      var v = this.value.trim().toLowerCase();
      clearTimeout(qt); qt = setTimeout(function () { filters.q = v; applyFilters(); }, 180);
    });
    $('layer-counties').addEventListener('click', function () { setLayerMode('counties'); });
    $('layer-dots').addEventListener('click', function () { setLayerMode('dots'); });
    $('scale-national').addEventListener('click', function () {
      scaleMode = 'national'; this.classList.add('active'); $('scale-screen').classList.remove('active'); if (mapReady) updateChoropleth();
    });
    $('scale-screen').addEventListener('click', function () {
      scaleMode = 'screen'; this.classList.add('active'); $('scale-national').classList.remove('active'); if (mapReady) updateChoropleth();
    });
    $('place-form').addEventListener('submit', function (e) { e.preventDefault(); searchPlace($('place-search').value.trim()); });
    $('btn-image').addEventListener('click', function () {
      if (!mapReady) return;
      map.once('idle', function () {
        map.getCanvas().toBlob(function (blob) {
          var a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = 'black-business-research-table-map.png';
          document.body.appendChild(a); a.click(); a.remove(); setTimeout(function () { URL.revokeObjectURL(a.href); }, 2000);
        });
      });
      map.triggerRepaint();
    });
    $('btn-default').addEventListener('click', function () {
      EXPANDED_COLS.forEach(function (i) { table.column(i).visible(false); });
      this.classList.add('active'); $('btn-expanded').classList.remove('active');
    });
    $('btn-expanded').addEventListener('click', function () {
      EXPANDED_COLS.forEach(function (i) { table.column(i).visible(true); });
      this.classList.add('active'); $('btn-default').classList.remove('active');
    });
    $('btn-guide').addEventListener('click', openGuide);
    $('guide-close').addEventListener('click', closeGuide);
    $('guide-go').addEventListener('click', closeGuide);
    $('guide').addEventListener('click', function (e) { if (e.target === this) closeGuide(); });
    document.addEventListener('keydown', function (e) { if (e.key === 'Escape' && !$('guide').hidden) closeGuide(); });
    wireForm('dataset-form', 'form-success', 'Submit request →');
    wireForm('suggest-form', 'suggest-success', 'Send suggestion →');
  }

  // ── Boot ──────────────────────────────────────────────────────────────────
  function boot() {
    Promise.all([
      fetchText('data/businesses.csv'),
      fetchText('data/sources.csv'),
      fetch('data/meta.json').then(function (r) { return r.ok ? r.json() : {}; }).catch(function () { return {}; }),
      fetch('data/geo/counties-10m.json').then(function (r) { return r.json(); }),
      fetch('data/us-states.geojson').then(function (r) { return r.json(); })
    ]).then(function (res) {
      rows = parseCsv(res[0]).filter(function (r) { return r.business_name; });
      sources = parseCsv(res[1]);
      meta = res[2] || {};
      statesGeo = res[4];
      var countiesGeo = prepareCounties(res[3]);
      computeCoverage();
      prepareRows();
      renderCoverageText();
      renderSourcesTable();
      populateStateFilter();
      current = rows.filter(function (r) { return passes(r, false); });
      initTable();
      wireControls();
      initMap(countiesGeo);
      dataReady = true;
      applyFilters();
      maybeShowGuideOnce();
    }).catch(function (err) {
      console.error('Could not load site data:', err);
      var ld = $('map-loading'); if (ld) ld.textContent = 'Could not load the data. Please refresh, or download the CSV directly.';
      setText('stat-footnote', 'The data could not be loaded. You can still download it directly from the Data section.');
    });
  }

  window.BBRT = { map: function () { return map; }, selectCounty: selectCounty, counts: function () { return countsByFips; } };   // QA hook
  document.addEventListener('DOMContentLoaded', boot);
}());
