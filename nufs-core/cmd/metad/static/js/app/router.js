// NUFS console — lightweight hash router. Maps location.hash to a page
// component key; the root app renders the matching component. Kept tiny and
// framework-free (no vue-router) to preserve the zero-build property.
(function () {
  'use strict';
  var R = window.NUFS.Router = {};

  // route: key → { key, label } ; params: { id? }
  R.current = { key: 'overview', label: 'Overview', params: {} };

  R.PAGES = [
    { key: 'overview', label: 'Overview', href: '#/overview' },
    { key: 'nodes', label: 'Nodes', href: '#/nodes' },
    { key: 'repair', label: 'Repair', href: '#/repair' },
    { key: 'rebalance', label: 'Rebalance', href: '#/rebalance' },
    { key: 'namespace', label: 'Namespace', href: '#/namespace' },
    { key: 'backups', label: 'Backups', href: '#/backups' }
  ];

  function parse(hash) {
    hash = hash || '';
    var path = hash.replace(/^#\/?/, '').replace(/\/+$/, '');
    var parts = path.split('/').filter(Boolean);
    var params = {};
    var key = 'overview';

    if (parts.length >= 1) key = parts[0];
    if (parts.length >= 3 && parts[0] === 'nodes') params.id = parts[1];

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
    else if (key === 'overview') target = '#/overview';
    else target = '#' + key; // '#/nodes', '#/repair', ...
    if (window.location.hash === target) { apply(); } else { window.location.hash = target; }
  };

  R.start = function () {
    window.addEventListener('hashchange', apply);
    apply();
  };

  // titleFor(page key) → human label for topbar
  R.titleFor = function (pageKey) {
    var map = {
      overview: 'Cluster Overview',
      nodes: 'Nodes',
      node_detail: 'Node Detail',
      repair: 'Repair Queue',
      rebalance: 'Rebalance',
      namespace: 'Namespace Browser',
      backups: 'Backups'
    };
    return map[pageKey] || 'NUFS';
  };
})();
