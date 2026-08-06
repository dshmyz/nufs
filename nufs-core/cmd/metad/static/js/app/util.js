// NUFS console — shared pure helpers (no Vue dependency).
(function () {
  'use strict';
  // Bootstrap the global namespace consumed by every other app script.
  window.NUFS = window.NUFS || {};
  var U = window.NUFS.Util = {};

  // humanBytes: "211 KB", "1.4 GB", "2.04 TB"
  U.humanBytes = function (b) {
    if (b === null || b === undefined || b < 0) return '-';
    var units = ['B', 'KB', 'MB', 'GB', 'TB'];
    var i = 0, n = Number(b);
    while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
    return (i === 0 ? n : n.toFixed(n >= 100 ? 0 : n >= 10 ? 1 : 2)) + ' ' + units[i];
  };

  // state → CSS class + label, mirroring the Go helpers the console used to
  // share. NodeInfo.state arrives as a numeric enum (0=online,1=draining,
  // 2=maint,3=offline,4=failed); accept both numbers and strings for safety.
  var STATE_NUM = ['online', 'draining', 'maintenance', 'offline', 'failed'];
  U.stateName = function (s) {
    if (typeof s === 'number') return STATE_NUM[s] || 'unknown';
    if (!s) return 'unknown';
    return String(s).toLowerCase();
  };
  U.stateClass = function (s) {
    var n = U.stateName(s);
    switch (n) {
      case 'online': return 'success';
      case 'draining': case 'maintenance': return 'warning';
      case 'offline': return 'secondary';
      case 'failed': return 'danger';
      default: return 'secondary';
    }
  };
  U.stateLabel = function (s, numeric) {
    var n = numeric || typeof s === 'number' ? U.stateName(s) : String(s || 'unknown');
    // numeric path -> capitalize the canonical name
    if ((numeric || typeof s === 'number') && STATE_NUM.indexOf(n) >= 0) {
      return n.charAt(0).toUpperCase() + n.slice(1);
    }
    var c = String(s || 'Unknown');
    return c.charAt(0).toUpperCase() + c.slice(1);
  };

  U.tierClass = function (t) {
    switch (t) {
      case 'Hot': case 'hot': return 'primary';
      case 'Warm': case 'warm': return 'info';
      case 'Cold': case 'cold': return 'secondary';
      default: return 'secondary';
    }
  };

  // fmtTime: "2026-08-06 15:04:05" from a unix-seconds or unix-nano timestamp.
  U.fmtTime = function (ts, nano) {
    if (!ts || ts === 0) return '-';
    var ms = nano ? Math.floor(ts / 1e6) : ts * 1000;
    var d = new Date(ms);
    if (isNaN(d.getTime())) return '-';
    function p(n) { return n < 10 ? '0' + n : '' + n; }
    return d.getFullYear() + '-' + p(d.getMonth() + 1) + '-' + p(d.getDate()) +
      ' ' + p(d.getHours()) + ':' + p(d.getMinutes()) + ':' + p(d.getSeconds());
  };

  // relTime: compact "3s / 14m / 2h ago" for heartbeats.
  U.relTime = function (ts, nano) {
    if (!ts || ts === 0) return '-';
    var ms = nano ? Math.floor(ts / 1e6) : ts * 1000;
    var diff = Date.now() - ms;
    if (diff < 0) return 'now';
    var s = Math.floor(diff / 1000);
    if (s < 60) return s + 's ago';
    var m = Math.floor(s / 60);
    if (m < 60) return m + 'm ago';
    var h = Math.floor(m / 60);
    if (h < 24) return h + 'h ago';
    return Math.floor(h / 24) + 'd ago';
  };

  // clamp
  U.clamp = function (v, lo, hi) { return v < lo ? lo : v > hi ? hi : v; };
})();
