// NUFS Admin — shared utilities

// Table search filter
function filterTable(inputId, tableId) {
  var q = document.getElementById(inputId).value.toLowerCase();
  var rows = document.getElementById(tableId).querySelectorAll('tbody tr');
  for (var i = 0; i < rows.length; i++) {
    rows[i].style.display = q === '' ? '' : (rows[i].textContent.toLowerCase().indexOf(q) > -1 ? '' : 'none');
  }
}

// Table sorting (click on <th data-sort="int|string">)
document.addEventListener('click', function(e) {
  var th = e.target.closest('th');
  if (!th || !th.hasAttribute('data-sort')) return;
  var table = th.closest('table');
  var tbody = table.querySelector('tbody');
  var rows = Array.from(tbody.querySelectorAll('tr'));
  var idx = Array.from(th.parentNode.children).indexOf(th);
  var type = th.getAttribute('data-sort');
  var asc = !th.classList.contains('sort-asc');

  // Reset all th's in this table
  th.parentNode.querySelectorAll('th').forEach(function(h) { h.classList.remove('sort-asc', 'sort-desc'); });
  th.classList.add(asc ? 'sort-asc' : 'sort-desc');

  rows.sort(function(a, b) {
    var va = a.children[idx] ? a.children[idx].textContent.trim() : '';
    var vb = b.children[idx] ? b.children[idx].textContent.trim() : '';
    if (type === 'int') { va = parseInt(va) || 0; vb = parseInt(vb) || 0; }
    else { va = va.toLowerCase(); vb = vb.toLowerCase(); }
    if (va < vb) return asc ? -1 : 1;
    if (va > vb) return asc ? 1 : -1;
    return 0;
  });
  rows.forEach(function(r) { tbody.appendChild(r); });
});

// Time formatting helpers (used in templates)
function formatUnix(ts) {
  if (!ts || ts === 0) return '-';
  var d = new Date(Math.floor(ts / 1000000)); // nanosec → ms
  if (isNaN(d.getTime())) return '-';
  return d.toLocaleString();
}

// ===== Real-time metrics via SSE =====
(function() {
  var chartData = [];
  var MAX_POINTS = 60;
  var chartCanvas = document.getElementById('ops-chart');
  if (!chartCanvas) return; // only on overview page

  var es = new EventSource('/admin/metrics/stream');

  es.onmessage = function(e) {
    try {
      var d = JSON.parse(e.data);
      updateStats(d);
      updateChart(d);
    } catch(err) {
      console.error('metrics parse error:', err);
    }
  };

  es.onerror = function() {
    // EventSource auto-reconnects
  };

  function updateStats(d) {
    setText('stat-nodes-total', d.nodes_total);
    var subs = [];
    if (d.nodes_online > 0) subs.push('<span class="stat-green">' + d.nodes_online + ' online</span>');
    if (d.nodes_draining > 0) subs.push('<span class="stat-yellow">' + d.nodes_draining + ' draining</span>');
    if (d.nodes_offline > 0) subs.push('<span class="stat-red">' + d.nodes_offline + ' offline</span>');
    setHTML('stat-nodes-sub', subs.join(' &middot; '));
    setText('stat-buckets', d.buckets_total);
    setText('stat-chunks', d.chunks_total);
    setText('stat-repairs', d.repair_count);
    setText('stat-ops-rate', d.ops_rate.toFixed(1));

    var fill = document.getElementById('capacity-fill');
    if (fill) fill.style.width = Math.round(d.capacity_pct) + '%';

    setText('capacity-used', d.used_gb + ' GB used');
    setText('capacity-total', d.capacity_gb + ' GB total (' + d.capacity_pct.toFixed(1) + '%)');
  }

  function setText(id, val) {
    var el = document.getElementById(id);
    if (el) el.textContent = val;
  }

  function setHTML(id, html) {
    var el = document.getElementById(id);
    if (el) el.innerHTML = html;
  }

  // ===== Chart =====
  function updateChart(d) {
    chartData.push(d.ops_rate);
    if (chartData.length > MAX_POINTS) chartData.shift();
    drawChart();
  }

  function drawChart() {
    var ctx = chartCanvas.getContext('2d');
    var rect = chartCanvas.parentElement.getBoundingClientRect();
    var dpr = window.devicePixelRatio || 1;
    var w = Math.max(rect.width - 40, 200);
    var h = 220;
    chartCanvas.width = w * dpr;
    chartCanvas.height = h * dpr;
    chartCanvas.style.width = w + 'px';
    chartCanvas.style.height = h + 'px';
    ctx.scale(dpr, dpr);

    var pad = { top: 10, bottom: 28, left: 50, right: 16 };
    var plotW = w - pad.left - pad.right;
    var plotH = h - pad.top - pad.bottom;

    ctx.clearRect(0, 0, w, h);

    if (chartData.length < 2) {
      ctx.fillStyle = '#94a3b8';
      ctx.font = '13px Inter, sans-serif';
      ctx.textAlign = 'center';
      ctx.fillText('Waiting for data...', w / 2, h / 2 + 4);
      return;
    }

    // Find max
    var max = 0;
    for (var i = 0; i < chartData.length; i++) {
      if (chartData[i] > max) max = chartData[i];
    }
    if (max < 0.01) max = 1;
    var mag = Math.pow(10, Math.floor(Math.log10(max)));
    var nice = Math.ceil(max / mag) * mag;
    if (nice <= 0) nice = 1;
    max = nice;

    // Grid lines
    ctx.strokeStyle = '#e9edf2';
    ctx.lineWidth = 1;
    ctx.font = '11px Inter, sans-serif';
    ctx.fillStyle = '#94a3b8';
    ctx.textAlign = 'right';

    var gridLines = 4;
    for (var i = 0; i <= gridLines; i++) {
      var y = pad.top + (plotH / gridLines) * i;
      ctx.beginPath();
      ctx.moveTo(pad.left, Math.round(y) + 0.5);
      ctx.lineTo(w - pad.right, Math.round(y) + 0.5);
      ctx.stroke();
      var val = max - (max / gridLines) * i;
      ctx.fillText(val.toFixed(val < 1 ? 2 : 1), pad.left - 8, y + 4);
    }

    // Y axis label
    ctx.save();
    ctx.fillStyle = '#94a3b8';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.translate(14, pad.top + plotH / 2);
    ctx.rotate(-Math.PI / 2);
    ctx.fillText('ops/s', 0, 0);
    ctx.restore();

    // X axis tick labels
    ctx.fillStyle = '#94a3b8';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'top';
    ctx.fillText('now', w - pad.right, h - 22);
    ctx.fillText('-' + (chartData.length * 2) + 's', pad.left, h - 22);

    // Area fill
    var stepX = plotW / (chartData.length - 1);
    ctx.beginPath();
    for (var i = 0; i < chartData.length; i++) {
      var x = pad.left + i * stepX;
      var y = pad.top + plotH - (Math.min(chartData[i], max) / max) * plotH;
      if (i === 0) ctx.moveTo(x, pad.top + plotH);
      ctx.lineTo(x, y);
    }
    ctx.lineTo(pad.left + (chartData.length - 1) * stepX, pad.top + plotH);
    ctx.closePath();
    ctx.fillStyle = 'rgba(59, 130, 246, 0.06)';
    ctx.fill();

    // Line
    ctx.beginPath();
    for (var i = 0; i < chartData.length; i++) {
      var x = pad.left + i * stepX;
      var y = pad.top + plotH - (Math.min(chartData[i], max) / max) * plotH;
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    }
    ctx.strokeStyle = '#3b82f6';
    ctx.lineWidth = 2;
    ctx.lineJoin = 'round';
    ctx.lineCap = 'round';
    ctx.stroke();

    // Current value dot
    if (chartData.length > 0) {
      var lastX = pad.left + (chartData.length - 1) * stepX;
      var lastY = pad.top + plotH - (Math.min(chartData[chartData.length - 1], max) / max) * plotH;
      ctx.beginPath();
      ctx.arc(lastX, lastY, 4, 0, Math.PI * 2);
      ctx.fillStyle = '#3b82f6';
      ctx.fill();
      ctx.beginPath();
      ctx.arc(lastX, lastY, 6, 0, Math.PI * 2);
      ctx.strokeStyle = 'rgba(59, 130, 246, 0.2)';
      ctx.lineWidth = 2;
      ctx.stroke();
    }
  }

  // Redraw on resize
  window.addEventListener('resize', function() {
    if (chartData.length > 0) drawChart();
  });
})();
