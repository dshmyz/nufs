// NUFS console — lightweight hash router. Maps location.hash to a page
// component key; the root app renders the matching component. Kept tiny and
// framework-free (no vue-router) to preserve the zero-build property.
(function () {
  'use strict';
  var R = window.NUFS.Router = {};

  // route: key → { key, labelKey } ; params: { id? }
  R.current = { key: 'overview', labelKey: 'page.overview', params: {} };

  R.PAGES = [
    { key: 'overview', labelKey: 'page.overview', href: '#/overview' },
    { key: 'nodes', labelKey: 'page.nodes', href: '#/nodes' },
    { key: 'repair', labelKey: 'page.repair', href: '#/repair' },
    { key: 'rebalance', labelKey: 'page.rebalance', href: '#/rebalance' },
    { key: 'namespace', labelKey: 'page.namespace', href: '#/namespace' },
    { key: 'chunks', labelKey: 'page.chunks', href: '#/chunks' },
    { key: 'quota', labelKey: 'page.quota', href: '#/quota' },
    { key: 'ops', labelKey: 'page.ops', href: '#/ops' },
    { key: 'backups', labelKey: 'page.backups', href: '#/backups' }
  ];

  function parse(hash) {
    hash = hash || '';
    // Strip any ?query so #/namespace?root=N still routes to 'namespace'; the
    // page (namespace) reads the query itself for extra params like root inode.
    var noQuery = hash.split('?')[0];
    var path = noQuery.replace(/^#\/?/, '').replace(/\/+$/, '');
    var parts = path.split('/').filter(Boolean);
    var params = {};
    var key = 'overview';

    if (parts.length >= 1) key = parts[0];

    // Nested detail routes:
    //   #/nodes/<id>    → node_detail
    //   #/chunks/<id>   → chunk_detail (single chunk's metadata / replicas)
    //   #/buckets/<name> → bucket_detail
    if (parts.length >= 2 && parts[0] === 'nodes') { key = 'node_detail'; params.id = parts[1]; }
    else if (parts.length >= 2 && parts[0] === 'chunks') { key = 'chunk_detail'; params.id = parts[1]; }
    else if (parts.length >= 2 && parts[0] === 'buckets') { key = 'bucket_detail'; params.id = decodeURIComponent(parts[1]); }

    return { key: key, params: params };
  }

  function apply() {
    var next = parse(window.location.hash);
    R.current.key = next.key;
    R.current.params = next.params;
    R.onChange && R.onChange(R.current);
  }

  R.navigate = function (key, params) {
    var p = params || {};
    var target;
    if (key === 'node_detail' && p.id) target = '#/nodes/' + p.id;
    else if (key === 'chunk_detail' && p.id) target = '#/chunks/' + p.id;
    else if (key === 'bucket_detail' && p.id) target = '#/buckets/' + encodeURIComponent(p.id);
    else if (key === 'overview') target = '#/overview';
    else target = '#' + key; // '#/nodes', '#/repair', ...
    if (window.location.hash === target) { apply(); } else { window.location.hash = target; }
  };

  R.start = function () {
    window.addEventListener('hashchange', apply);
    apply();
  };

  // titleFor(page key) → i18n key for the topbar title. The app layer renders
  // it with t(); localizing here in the router keeps the map near the labels.
  R.titleFor = function (pageKey) {
    var map = {
      overview: 'page.overview',
      nodes: 'page.nodes',
      node_detail: 'page.node_detail',
      chunk_detail: 'page.chunk_detail',
      bucket_detail: 'page.bucket_detail',
      repair: 'page.repair',
      rebalance: 'page.rebalance',
      namespace: 'page.namespace',
      chunks: 'page.chunks',
      quota: 'page.quota',
      ops: 'page.ops',
      backups: 'page.backups'
    };
    return map[pageKey] || 'app.metad';
  };
})();
