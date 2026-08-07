// NUFS console — root Vue application. Registers every shared component
// globally, renders the AppLayout (sidebar + topbar), and swaps the active
// page component in response to the hash router.
(function () {
  'use strict';
  var Vue = window.Vue;
  var R = window.NUFS.Router;
  var C = window.NUFS.Components;
  var P = window.NUFS.Pages;
  var I = window.NUFS.I18n;

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
      title: function () { return I.t(R.titleFor(this.state.key)); },
      routedComponent: function () {
        // Return the component object (not a name string) so detail pages map
        // cleanly: route key 'node_detail' → P.nodeDetail. Top-level routes
        // (overview, nodes, ...) already share their key with the page name.
        var k = this.state.key;
        if (k === 'node_detail') return P.nodeDetail;
        return P[k];
      }
    },
    methods: {
      isActive: function (k) {
        var pk = this.state.key;
        return pk === k || (k === 'nodes' && pk === 'node_detail') || (k === 'chunks' && pk === 'chunk_detail');
      },
      icon: function (k) { return ICONS[k] || '•'; },
      // i18n helpers — exposed on the layout so the topbar toggle can flip the
      // reactive locale; page templates use the global `t` from globalProperties.
      t: I.t,
      lang: function () { return I.current(); },
      setLang: function (l) { I.setLocale(l); }
    },
    template: `
      <div class="layout">
        <aside class="sidebar">
          <div class="sidebar-header">
            <div class="sidebar-logo">NUFS</div>
            <span class="sidebar-subtitle">{{ t('app.brand_sub') }}</span>
          </div>
          <nav class="sidebar-nav">
            <a v-for="p in nav" :key="p.key" :href="p.href"
               class="side-link" :class="{ 'active': isActive(p.key) }">
              <span class="nav-icon">{{ icon(p.key) }}</span><span>{{ t(p.labelKey) }}</span>
            </a>
          </nav>
        </aside>

        <div class="main">
          <header class="topbar">
            <h2>{{ title }}</h2>
            <div class="topbar-right">
              <div class="lang-toggle" role="group" aria-label="Language / 语言">
                <button class="lang-btn" :class="{ 'active': lang() === 'zh' }" @click="setLang('zh')">中</button>
                <button class="lang-btn" :class="{ 'active': lang() === 'en' }" @click="setLang('en')">EN</button>
              </div>
              <span class="leader-badge leader-yes">{{ t('app.metad') }}</span>
              <span class="version-badge">{{ t('app.control_plane') }}</span>
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

  // Expose the i18n translator globally so every page template can call
  // t('key', {n: ...}) and re-render automatically when the locale flips.
  app.config.globalProperties.t = I.t;

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
