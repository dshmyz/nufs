// NUFS console — root Vue application. Registers every shared component
// globally, renders the AppLayout (sidebar + topbar), and swaps the active
// page component in response to the hash router.
(function () {
  'use strict';
  var Vue = window.Vue;
  var R = window.NUFS.Router;
  var C = window.NUFS.Components;
  var P = window.NUFS.Pages;

  // Make the router's current state reactive so the sidebar highlight and the
  // routed <component> update when the hash changes. The router mutates
  // R.current.key / R.current.params on each hashchange; wrapping the object
  // in Vue.reactive makes those mutations observable by the root app.
  R.state = Vue.reactive({ key: 'overview', params: {} });
  R.current = R.state;
  R.onChange = function (page) {
    R.state.key = page.key;
    R.state.params = page.params;
  };

  // ---- AppLayout: sidebar nav + topbar + routed page ----
  // Uses the existing stylesheet's .layout/.sidebar/.main structure (including
  // the responsive icon-rail collapse on mobile), so no new layout CSS.
  var ICONS = { overview: '◆', nodes: '◫', repair: '↻', rebalance: '⇄', namespace: '◈', chunks: '▣', quota: '◻', ops: '⌁', backups: '▣' };
  var AppLayout = {
    data: function () {
      return { state: R.state, nav: R.PAGES };
    },
    computed: {
      title: function () { return R.titleFor(this.state.key); },
      routedComponent: function () {
        var k = this.state.key;
        if (k === 'node_detail' || k === 'chunk_detail') return k;
        return k;
      }
    },
    methods: {
      isActive: function (k) {
        var pk = this.state.key;
        return pk === k || (k === 'nodes' && pk === 'node_detail') || (k === 'chunks' && pk === 'chunk_detail');
      },
      icon: function (k) { return ICONS[k] || '•'; }
    },
    template: `
      <div class="layout">
        <aside class="sidebar">
          <div class="sidebar-header">
            <div class="sidebar-logo">NUFS</div>
            <span class="sidebar-subtitle">distributed storage</span>
          </div>
          <nav class="sidebar-nav">
            <a v-for="p in nav" :key="p.key" :href="p.href"
               class="side-link" :class="{ 'active': isActive(p.key) }">
              <span class="nav-icon">{{ icon(p.key) }}</span><span>{{ p.label }}</span>
            </a>
          </nav>
        </aside>

        <div class="main">
          <header class="topbar">
            <h2>{{ title }}</h2>
            <div class="topbar-right">
              <span class="leader-badge leader-yes">metad</span>
              <span class="version-badge">control plane</span>
            </div>
          </header>
          <main class="content">
            <component :is="routedComponent" :id="state.params.id"></component>
          </main>
        </div>
        <ToastRoot/>
      </div>
    `
  };

  // ---- build the app ----
  var app = Vue.createApp({
    components: { AppLayout: AppLayout },
    template: '<AppLayout/>'
  });

  // Expose the shared util helpers on every instance so templates can call
  // U.humanBytes(...) / U.relTime(...) directly. Without this, the compiled
  // template render functions can't see the IIFE-closure `U` variable — they
  // only resolve identifiers against the component instance (_ctx) or globals.
  app.config.globalProperties.U = window.NUFS.Util;

  // Register all shared components globally so page templates can use
  // <ToastRoot/>, <Modal/>, <StateBadge/>, etc. without per-page imports.
  Object.keys(C).forEach(function (name) {
    if (name !== 'toast' && C[name] && typeof C[name] === 'object' && C[name].template) {
      app.component(name, C[name]);
    }
  });

  // Register page components.
  Object.keys(P).forEach(function (name) {
    app.component(name, P[name]);
  });

  app.mount('#app');
  R.start();
})();
