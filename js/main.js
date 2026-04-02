(function () {
  'use strict';

  var map, table;

  // ── Map ────────────────────────────────────────────────────────────────
  function initMap() {
    map = L.map('map').setView([40.73, -73.93], 11);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
      maxZoom: 18
    }).addTo(map);
  }

  function addMarkersToMap(rows) {
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

      var websiteHtml = row.website
        ? '<div class="popup-website"><a href="' + row.website +
          '" target="_blank" rel="noopener">' +
          row.website.replace(/^https?:\/\//, '').split('/')[0] + ' \u2197</a></div>'
        : '';

      var popupHtml =
        '<div class="popup-name">' + row.business_name + '</div>' +
        '<div class="popup-meta">' + (row.industry || '') + ' \u00b7 ' + (row.address_city || '') + '</div>' +
        (row.owner_name ? '<div class="popup-field"><span class="popup-label">Owner: </span>' + row.owner_name + '</div>' : '') +
        (row.year_founded ? '<div class="popup-field"><span class="popup-label">Founded: </span>' + row.year_founded + '</div>' : '') +
        (desc ? '<div class="popup-field"><span class="popup-label">About: </span>' + desc + '</div>' : '') +
        websiteHtml;

      marker.bindPopup(popupHtml, { maxWidth: 260 });
      marker.addTo(map);
    });
  }

  // ── Table (stub — filled in Task 7) ────────────────────────────────────
  function initTable(rows) {
    // placeholder — implemented in Task 7
    console.log('Loaded', rows.length, 'businesses');
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
      },
      error: function (err) {
        console.error('Could not load businesses.csv:', err);
      }
    });
  }

  // ── Init ─────────────────────────────────────────────────────────────────
  initMap();
  loadData();

}());
