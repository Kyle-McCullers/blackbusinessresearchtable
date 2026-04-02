(function () {
  'use strict';

  var map, markerLayer;
  var table; // assigned in initTable

  // ── Utilities ────────────────────────────────────────────────────────────
  function escHtml(s) {
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function safeUrl(url) {
    return /^https?:\/\//i.test(url) ? url : '';
  }

  // ── Map ────────────────────────────────────────────────────────────────
  function initMap() {
    map = L.map('map').setView([40.73, -73.93], 11);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
      maxZoom: 18
    }).addTo(map);
    markerLayer = L.layerGroup().addTo(map);
  }

  function addMarkersToMap(rows) {
    markerLayer.clearLayers();
    rows.forEach(function (row) {
      var lat = parseFloat(row.latitude);
      var lon = parseFloat(row.longitude);
      if (isNaN(lat) || isNaN(lon)) return;

      var marker = L.circleMarker([lat, lon], {
        radius: 6,
        fillColor: '#1B4332',
        color: '#ffffff',
        weight: 1.5,
        opacity: 1,
        fillOpacity: 0.85
      });

      var desc = row.description
        ? (row.description.length > 120
            ? row.description.substring(0, 120) + '\u2026'
            : row.description)
        : '';

      var href = safeUrl(row.website);
      var websiteHtml = href
        ? '<div class="popup-website"><a href="' + escHtml(href) +
          '" target="_blank" rel="noopener">' +
          escHtml(href.replace(/^https?:\/\//, '').split('/')[0]) + ' \u2197</a></div>'
        : '';

      var popupHtml =
        '<div class="popup-name">' + escHtml(row.business_name) + '</div>' +
        '<div class="popup-meta">' + escHtml(row.industry || '') + ' \u00b7 ' + escHtml(row.address_city || '') + '</div>' +
        (row.owner_name ? '<div class="popup-field"><span class="popup-label">Owner: </span>' + escHtml(row.owner_name) + '</div>' : '') +
        (row.year_founded ? '<div class="popup-field"><span class="popup-label">Founded: </span>' + escHtml(row.year_founded) + '</div>' : '') +
        (desc ? '<div class="popup-field"><span class="popup-label">About: </span>' + escHtml(desc) + '</div>' : '') +
        websiteHtml;

      marker.bindPopup(popupHtml, { maxWidth: 260 });
      marker.addTo(markerLayer);
    });
  }

  // ── Table ───────────────────────────────────────────────────────────────
  function initTable(rows) {
    table = $('#business-table').DataTable({
      data: rows,
      pageLength: 25,
      lengthChange: false,
      autoWidth: false,
      dom: 'tip',
      columnDefs: [{ targets: [7, 8, 9, 10], visible: false }],
      columns: [
        { data: 'business_name',  title: 'Business Name' },
        { data: 'owner_name',     title: 'Owner',         defaultContent: '\u2014' },
        { data: 'address_city',   title: 'City',          defaultContent: '\u2014' },
        { data: 'address_state',  title: 'State',         defaultContent: '\u2014' },
        { data: 'industry',       title: 'Industry',      defaultContent: '\u2014' },
        { data: 'year_founded',   title: 'Founded',       defaultContent: '\u2014' },
        {
          data: 'website',
          title: 'Website',
          defaultContent: '\u2014',
          orderable: false,
          render: function (data) {
            var href = safeUrl(data);
            if (!href) return '\u2014';
            var display = href.replace(/^https?:\/\//, '').split('/')[0];
            return '<a href="' + escHtml(href) + '" target="_blank" rel="noopener">' + escHtml(display) + ' \u2197</a>';
          }
        },
        // Expanded columns (indices 7–10, hidden by default)
        { data: 'address_street', title: 'Address',       defaultContent: '\u2014' },
        { data: 'certification',  title: 'Certification', defaultContent: '\u2014' },
        { data: 'naics_code',     title: 'NAICS Code',    defaultContent: '\u2014' },
        {
          data: 'description',
          title: 'Description',
          defaultContent: '\u2014',
          render: function (data) {
            if (!data) return '\u2014';
            var truncated = data.length > 150 ? data.substring(0, 150) + '\u2026' : data;
            return escHtml(truncated);
          }
        }
      ]
    });
  }

  // ── Coverage bar ────────────────────────────────────────────────────────
  var US_BLACK_EMPLOYER_BUSINESSES = 160000; // 2021 Census Annual Business Survey

  function updateCoverageBar(count) {
    var pct = (count / US_BLACK_EMPLOYER_BUSINESSES * 100);
    var pctDisplay = pct.toFixed(1) + '%';
    var fillEl = document.getElementById('coverage-fill');
    var pctEl  = document.getElementById('coverage-pct');
    if (fillEl) fillEl.style.width = Math.min(pct, 100) + '%';
    if (pctEl)  pctEl.textContent  = pctDisplay;
  }

  // ── Data loading ────────────────────────────────────────────────────────
  function loadData() {
    Papa.parse('data/businesses.csv', {
      download: true,
      header: true,
      skipEmptyLines: true,
      complete: function (results) {
        var rows = results.data.filter(function (r) { return r.business_name; });
        var countEl = document.getElementById('stat-count');
        if (countEl) countEl.textContent = rows.length.toLocaleString();
        addMarkersToMap(rows);
        initTable(rows);
        updateCoverageBar(rows.length);
      },
      error: function (err) {
        console.error('Could not load businesses.csv:', err);
      }
    });
  }

  // ── Init ─────────────────────────────────────────────────────────────────
  document.addEventListener('DOMContentLoaded', function () {
    initMap();
    loadData();

    // ── Toggle ───────────────────────────────────────────────────────────────
    document.getElementById('btn-default').addEventListener('click', function () {
      if (!table) return;
      [7, 8, 9, 10].forEach(function (i) { table.column(i).visible(false); });
      document.getElementById('btn-default').classList.add('active');
      document.getElementById('btn-expanded').classList.remove('active');
    });

    document.getElementById('btn-expanded').addEventListener('click', function () {
      if (!table) return;
      [7, 8, 9, 10].forEach(function (i) { table.column(i).visible(true); });
      document.getElementById('btn-expanded').classList.add('active');
      document.getElementById('btn-default').classList.remove('active');
    });

    // ── Custom search ────────────────────────────────────────────────────────
    document.getElementById('table-search').addEventListener('keyup', function () {
      if (table) table.search(this.value).draw();
    });

    // ── Request Dataset button ────────────────────────────────────────────────
    document.getElementById('btn-request').addEventListener('click', function () {
      document.getElementById('access').scrollIntoView({ behavior: 'smooth' });
    });

    // ── Access form ──────────────────────────────────────────────────────────
    document.getElementById('dataset-form').addEventListener('submit', function (e) {
      e.preventDefault();
      var name        = document.getElementById('req-name').value;
      var affiliation = document.getElementById('req-affiliation').value;
      var use         = document.getElementById('req-use').value;
      var subject = encodeURIComponent('Black Business Research Table \u2014 Dataset Request');
      var body    = encodeURIComponent(
        'Name: ' + name + '\n' +
        'Affiliation: ' + affiliation + '\n' +
        'Intended Use:\n' + use
      );
      window.location.href = 'mailto:kylemcc@umich.edu?subject=' + subject + '&body=' + body;
    });
  });

}());
