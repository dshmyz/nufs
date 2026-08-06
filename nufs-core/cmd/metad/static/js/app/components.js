// NUFS console — shared Vue 3 components. All are plain JS objects (no .vue
// SFCs) registered globally by app.js, per the zero-build delivery choice.
// They reuse the design system in static/css/style.css (rack sidebar, amber
// capacity heat, cyan data plane, graphite hero).
(function () {
  'use strict';
  var Vue = window.Vue;
  var U = window.NUFS.Util;
  var C = window.NUFS.Components = {};

  // ---- Toast system (lightweight global store + component) ----
  var toasts = Vue.reactive([]);
  function pushToast(msg, kind, ttl) {
    var id = Date.now() + Math.random();
    toasts.push({ id: id, msg: msg, kind: kind || 'ok' });
    setTimeout(function () {
      var i = toasts.findIndex(function (t) { return t.id === id; });
      if (i >= 0) toasts.splice(i, 1);
    }, ttl || 3500);
  }
  C.toast = { ok: function (m) { pushToast(m, 'ok'); }, err: function (m) { pushToast(m, 'err', 6000); }, list: toasts };

  C.ToastRoot = {
    template: '<div class="toast-root"><transition-group name="toast">' +
      '<div v-for="t in toasts" :key="t.id" class="toast" :class="\'toast-\' + t.kind">{{ t.msg }}</div>' +
      '</transition-group></div>',
    computed: { toasts: function () { return C.toast.list; } }
  };

  // ---- State badge ----
  C.StateBadge = {
    props: ['state'],
    computed: {
      cls: function () { return U.stateClass(this.state); },
      label: function () { return U.stateLabel(this.state); }
    },
    template: '<span class="badge" :class="\'badge-\' + cls">{{ label }}</span>'
  };

  // ---- Loading placeholder ----
  C.Loading = {
    template: '<div class="loading">Loading…</div>'
  };

  // ---- Empty / placeholder ----
  C.Empty = {
    props: ['title', 'hint'],
    template: '<div class="ops-empty"><div class="ops-empty-title">{{ title }}</div>' +
      '<div v-if="hint" class="ops-empty-hint">{{ hint }}</div>' +
      '<slot></slot></div>'
  };

  // ---- RingGauge: SVG progress ring (used for per-node capacity) ----
  // props: pct (0-100), size, color($ primary|warning|danger)
  C.RingGauge = {
    props: { pct: { type: Number, default: 0 }, size: { type: Number, default: 44 }, color: { type: String, default: 'primary' } },
    computed: {
      r: function () { return this.size / 2 - 3; },
      circ: function () { return 2 * Math.PI * this.r; },
      offset: function () { var u = Math.max(0, Math.min(100, this.pct)) / 100; return this.circ - this.circ * u; },
      strokeCls: function () { return 'ring-var-' + this.color; }
    },
    template: '<svg class="ring-gauge" :width="size" :height="size" :viewBox="\'0 0 \' + size + \' \' + size">' +
      '<circle :cx="size/2" :cy="size/2" :r="r" class="ring-track"/>' +
      '<circle :cx="size/2" :cy="size/2" :r="r" class="ring-fill" :class="strokeCls" ' +
      ':stroke-dasharray="circ" :stroke-dashoffset="offset"/>' +
      '</svg>'
  };

  // ---- TrendChart: canvas line/area chart (ops-rate live) ----
  C.TrendChart = {
    props: { series: { type: Array, default: function () { return []; } }, height: { type: Number, default: 220 }, color: { type: String, default: '#0891b2' }, ylabel: { type: String, default: 'ops/s' } },
    template: '<div class="trend-wrap"><canvas class="trend-canvas" :height="height"></canvas></div>',
    mounted: function () {
      var self = this;
      this._draw = function () { self.draw(); };
      window.addEventListener('resize', this._draw);
      this.$watch('series', this._draw, { deep: true });
    },
    beforeUnmount: function () { window.removeEventListener('resize', this._draw); },
    methods: {
      draw: function () {
        var canvas = this.$el.querySelector('canvas');
        if (!canvas || !canvas.getContext) return;
        var ctx = canvas.getContext('2d');
        var rect = this.$el.getBoundingClientRect();
        var dpr = window.devicePixelRatio || 1;
        var w = Math.max(rect.width - 8, 120);
        var h = this.height;
        canvas.width = w * dpr; canvas.height = h * dpr;
        canvas.style.width = w + 'px'; canvas.style.height = h + 'px';
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

        var pad = { top: 12, bottom: 26, left: 46, right: 12 };
        var plotW = w - pad.left - pad.right, plotH = h - pad.top - pad.bottom;
        ctx.clearRect(0, 0, w, h);

        var data = (this.series || []).slice();
        if (data.length < 2) {
          ctx.fillStyle = '#94a3b8'; ctx.font = '13px Inter, sans-serif';
          ctx.textAlign = 'center'; ctx.fillText('Waiting for data…', w / 2, h / 2 + 4);
          return;
        }

        var max = Math.max.apply(null, data.concat([0.01]));
        var mag = Math.pow(10, Math.floor(Math.log10(max)));
        var nice = Math.ceil(max / mag) * mag; if (nice <= 0) nice = 1; max = nice;

        // grid
        ctx.strokeStyle = '#e5eaf0'; ctx.lineWidth = 1;
        ctx.font = '11px Inter, sans-serif'; ctx.fillStyle = '#8b95a3'; ctx.textAlign = 'right';
        var gl = 4;
        for (var i = 0; i <= gl; i++) {
          var y = pad.top + (plotH / gl) * i;
          ctx.beginPath(); ctx.moveTo(pad.left, Math.round(y) + 0.5); ctx.lineTo(w - pad.right, Math.round(y) + 0.5); ctx.stroke();
          var val = max - (max / gl) * i;
          ctx.fillText(val.toFixed(val < 1 ? 2 : 1), pad.left - 8, y + 4);
        }
        ctx.save();
        ctx.fillStyle = '#94a3b8'; ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
        ctx.translate(12, pad.top + plotH / 2); ctx.rotate(-Math.PI / 2);
        ctx.fillText(this.ylabel, 0, 0); ctx.restore();

        var stepX = plotW / (data.length - 1);
        // area
        ctx.beginPath();
        for (var j = 0; j < data.length; j++) {
          var x = pad.left + j * stepX;
          var yv = pad.top + plotH - (Math.min(data[j], max) / max) * plotH;
          if (j === 0) ctx.moveTo(x, pad.top + plotH);
          ctx.lineTo(x, yv);
        }
        ctx.lineTo(pad.left + (data.length - 1) * stepX, pad.top + plotH);
        ctx.closePath();
        ctx.fillStyle = this.hexToRgba(this.color, 0.08); ctx.fill();
        // line
        ctx.beginPath();
        for (var k = 0; k < data.length; k++) {
          var x2 = pad.left + k * stepX;
          var y2 = pad.top + plotH - (Math.min(data[k], max) / max) * plotH;
          if (k === 0) ctx.moveTo(x2, y2); else ctx.lineTo(x2, y2);
        }
        ctx.strokeStyle = this.color; ctx.lineWidth = 2; ctx.lineJoin = 'round'; ctx.lineCap = 'round'; ctx.stroke();
        // current dot
        var lx = pad.left + (data.length - 1) * stepX;
        var ly = pad.top + plotH - (Math.min(data[data.length - 1], max) / max) * plotH;
        ctx.beginPath(); ctx.arc(lx, ly, 4, 0, Math.PI * 2); ctx.fillStyle = this.color; ctx.fill();
        ctx.beginPath(); ctx.arc(lx, ly, 7, 0, Math.PI * 2); ctx.strokeStyle = this.hexToRgba(this.color, 0.25); ctx.lineWidth = 2; ctx.stroke();
      },
      hexToRgba: function (hex, a) {
        hex = String(hex || '').replace('#', '');
        if (hex.length === 3) hex = hex.split('').map(function (c) { return c + c; }).join('');
        var n = parseInt(hex, 16);
        return 'rgba(' + ((n >> 16) & 255) + ',' + ((n >> 8) & 255) + ',' + (n & 255) + ',' + a + ')';
      }
    }
  };

  // ---- CapacityBar: logical-vs-physical horizontal bar ----
  C.CapacityBar = {
    props: { pct: { type: Number, default: 0 } },
    template: '<div class="capacity-bar"><div class="capacity-fill" :style="{ width: Math.max(0, Math.min(100, pct)) + \'%\' }"></div></div>'
  };

  // ---- Modal: confirm/dialog with slots ----
  C.Modal = {
    props: { show: Boolean, title: String, width: { type: String, default: '440px' } },
    emits: ['close'],
    template:
      '<teleport to="body">' +
      '<transition name="modal">' +
      '<div v-if="show" class="modal-overlay" @click.self="$emit(\'close\')">' +
      '<div class="modal" :style="{ width: width }" role="dialog" aria-modal="true">' +
      '<div class="modal-header"><h3>{{ title }}</h3>' +
      '<button class="modal-close" aria-label="Close" @click="$emit(\'close\')">&times;</button></div>' +
      '<div class="modal-body"><slot></slot></div>' +
      '<div class="modal-footer"><slot name="footer"></slot></div>' +
      '</div></div></transition></teleport>'
  };

  // ---- HeroRing: cluster-health sector ring (signature element) ----
  // props: online, draining, offline, total → draws concentric arcs in SVG.
  C.HeroRing = {
    props: { online: { type: Number, default: 0 }, draining: { type: Number, default: 0 }, offline: { type: Number, default: 0 }, total: { type: Number, default: 0 } },
    computed: {
      segs: function () {
        var r = 52, C = 2 * Math.PI * r, start = 0, out = [];
        var total = this.total;
        var add = function (n, cls) {
          if (n <= 0 || !total) return;
          var frac = Math.min(1, n / total);
          var len = C * frac;
          out.push({ cls: cls, dash: len.toFixed(2), gap: (C - len).toFixed(2), rot: (start - 90).toFixed(1) });
          start += frac * 360;
        };
        add(this.online, 'ok'); add(this.draining, 'degraded'); add(this.offline, 'down');
        return out;
      },
      verdict: function () {
        if (this.total === 0) return { cls: 'down', label: 'No nodes', pct: 0 };
        if (this.offline > 0) return { cls: 'degraded', label: 'Degraded', pct: Math.round(this.online / this.total * 100) };
        return { cls: 'ok', label: 'Cluster healthy', pct: Math.round(this.online / this.total * 100) };
      }
    },
    template:
      '<div class="hero-ring">' +
      '<svg width="120" height="120" viewBox="0 0 120 120">' +
      '<circle class="hero-ring-track" cx="60" cy="60" r="52"/>' +
      '<circle v-for="(s,i) in segs" :key="i" class="hero-ring-seg" :class="s.cls" cx="60" cy="60" r="52" ' +
      ':stroke-dasharray="s.dash + \' \' + s.gap" :transform="\'rotate(\' + s.rot + \' 60 60)\'"/>' +
      '</svg>' +
      '<div class="hero-ring-center"><span class="hero-verdict" :class="verdict.cls">{{ verdict.pct }}%</span>' +
      '<span class="hero-verdict-sub">{{ verdict.label }}</span></div>' +
      '</div>'
  };

  // ---- FaultDomainStrip: rack/zone → bay leds ----
  C.FaultDomainStrip = {
    props: { domains: { type: Array, default: function () { return []; } } },
    template:
      '<div class="fd-strip">' +
      '<div v-for="d in domains" :key="d.name" class="fd-card">' +
      '<div class="fd-name"><span>{{ d.name }}</span><span class="fd-count">{{ d.total }}</span></div>' +
      '<div class="fd-body"><span v-for="(b,i) in d.bays" :key="i" class="fd-bay" :class="b"></span></div>' +
      '</div></div>'
  };

  // ---- PageHeader with actions ----
  C.PageHeader = {
    props: { title: String, sub: String },
    template: '<div class="page-head"><h2 class="page-title">{{ title }}</h2><div v-if="sub" class="page-sub">{{ sub }}</div><slot></slot></div>'
  };
})();
