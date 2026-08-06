// NUFS console — API layer. Thin promise-based wrapper over the /api/v1 JSON
// endpoints (mounted alongside /admin on the same mux/port by the metad
// process). All handlers share the same origin, so no CORS config is needed.
(function () {
  'use strict';
  var A = window.NUFS.API = {};

  function parseError(res, body) {
    if (body && body.error) return new Error(body.error);
    return new Error('HTTP ' + res.status);
  }

  // request(path, opts) → parsed JSON. Throws on non-2xx.
  function request(path, opts) {
    opts = opts || {};
    return fetch(path, {
      method: opts.method || 'GET',
      headers: opts.body ? { 'Content-Type': 'application/json' } : undefined,
      body: opts.body ? JSON.stringify(opts.body) : undefined
    }).then(function (res) {
      return res.text().then(function (txt) {
        var body = null;
        try { body = txt ? JSON.parse(txt) : null; } catch (e) { body = null; }
        if (!res.ok) throw parseError(res, body);
        return body;
      });
    });
  }

  // jsonGet(path): GET, tolerate 404/503 by returning null (callers show
  // placeholder rather than erroring on uninitialized subsystems).
  A.get = function (path) {
    return fetch(path).then(function (res) {
      if (res.status === 404 || res.status === 503) return null;
      return res.text().then(function (txt) {
        var body = null;
        try { body = txt ? JSON.parse(txt) : null; } catch (e) { body = null; }
        if (!res.ok) throw parseError(res, body);
        return body;
      });
    });
  };

  A.post = function (path, body) { return request(path, { method: 'POST', body: body }); };
  A.put = function (path, body) { return request(path, { method: 'PUT', body: body }); };
  A.del = function (path) { return request(path, { method: 'DELETE' }); };

  // ---- cluster / resource helpers ----
  A.clusterStatus = function () { return A.get('/api/v1/cluster/status'); };
  A.clusterReadiness = function () { return A.get('/api/v1/cluster/readiness'); };
  A.clusterBalance = function () { return A.get('/api/v1/cluster/balance'); };
  A.nodes = function () { return A.get('/api/v1/nodes'); };
  A.node = function (id) { return A.get('/api/v1/nodes/' + id); };
  A.buckets = function () { return A.get('/api/v1/buckets'); };
  A.chunks = function () { return A.get('/api/v1/chunks'); };
  A.repairQueue = function () { return A.get('/api/v1/repair/queue'); };
  A.repair = function (id) { return A.get('/api/v1/repair/' + id); };
  A.triggerRepair = function (chunkId) { return A.post('/api/v1/repair/trigger', chunkId ? { chunk_id: chunkId } : undefined); };
  A.triggerRebalance = function () { return A.post('/api/v1/rebalance/trigger'); };
  A.decommissionNode = function (id) { return A.post('/api/v1/nodes/' + id + '/decommission'); };

  // ---- namespace (parent-inode based) ----
  // ReadDir returns []DirEntry { inode, type, name } for a parent inode id.
  A.readDir = function (parent) {
    var q = parent != null ? '?parent=' + parent : '';
    return A.get('/api/v1/namespace/readdir' + q);
  };
  A.inode = function (id) { return A.get('/api/v1/inodes/' + id); };
  A.mkdir = function (parent, name) { return A.post('/api/v1/namespace/mkdir', { parent: parent, name: name }); };
  // file/namespace mutations (all POST, may 500 with a message on failure)
  A.createFile = function (parent, name) { return A.post('/api/v1/namespace/createfile', { parent: parent, name: name }); };
  A.unlink = function (parent, name) { return A.post('/api/v1/namespace/unlink', { parent: parent, name: name }); };
  A.rmdir = function (parent, name) { return A.post('/api/v1/namespace/rmdir', { parent: parent, name: name }); };
  A.renameEntry = function (oldParent, oldName, newParent, newName) {
    return A.post('/api/v1/namespace/rename', { old_parent: oldParent, old_name: oldName, new_parent: newParent, new_name: newName });
  };
  A.symlink = function (parent, name, target) { return A.post('/api/v1/namespace/symlink', { parent: parent, name: name, target: target }); };
  A.readlink = function (id) { return A.get('/api/v1/namespace/readlink?id=' + id); };

  // ---- bucket quota ----
  // GET .../quota → { bucket, quota|null, usage:{used_bytes,objects}, ratios }
  A.bucketQuota = function (name) { return A.get('/api/v1/buckets/' + encodeURIComponent(name) + '/quota'); };
  A.setBucketQuota = function (name, quota) { return A.put('/api/v1/buckets/' + encodeURIComponent(name) + '/quota', quota); };
  A.deleteBucketQuota = function (name) { return A.del('/api/v1/buckets/' + encodeURIComponent(name) + '/quota'); };
  A.checkBucketQuota = function (name, addB, addO) {
    return A.post('/api/v1/buckets/' + encodeURIComponent(name) + '/quota/check', { additional_bytes: addB, additional_objects: addO });
  };

  // ---- data-maintenance observability ----
  A.scrub = function () { return A.post('/api/v1/scrub'); };
  A.writeOpsStatus = function () { return A.get('/api/v1/write-ops/status'); };
  A.writeAttempts = function (state) {
    var q = '?state=' + encodeURIComponent(state) + '&limit=200';
    return A.get('/api/v1/write-attempts' + q);
  };
  A.writeAttempt = function (id) { return A.get('/api/v1/write-attempts/' + encodeURIComponent(id)); };
  A.audit = function (opts) {
    opts = opts || {};
    var q = '?start=' + (opts.start || 0) + '&end=' + (opts.end || 0) + '&limit=' + (opts.limit || 100);
    return A.get('/api/v1/audit' + q);
  };
  A.locks = function (inode) { return A.get('/api/v1/locks?inode=' + inode); };

  // ---- chunk-level ops ----
  A.chunksByInode = function (id) { return A.get('/api/v1/chunks?inode_id=' + id); };
  A.chunk = function (id) { return A.get('/api/v1/chunks/' + id); };
  A.chunkSeal = function (id) { return A.post('/api/v1/chunks/' + id + '/seal'); };
  A.chunkCommit = function (id, checksum) { return A.post('/api/v1/chunks/' + id + '/commit', { checksum: checksum }); };
  A.migrateReplica = function (chunkId, fromNode, toNode) {
    return A.post('/api/v1/chunks/migrate-replica', { chunk_id: chunkId, from_node: fromNode, to_node: toNode });
  };
  A.deleteChunk = function (id) { return A.del('/api/v1/chunks/' + id); };

  // ---- backups ----
  // backups/status → { status, catalog? }; backups → { tasks, catalog?, status }
  A.backupStatus = function () { return A.get('/api/v1/backups/status'); };
  A.backups = function () { return A.get('/api/v1/backups'); };
  A.createBackup = function () { return A.post('/api/v1/backups'); };
  A.verifyBackup = function (id) { return A.post('/api/v1/backups/' + encodeURIComponent(id) + '/verify'); };
  A.pruneBackups = function () { return A.post('/api/v1/backups/prune?dry_run=true'); };

  // seed demo data (creates nodes/buckets/names on first run)
  A.seed = function () { return A.post('/admin/seed'); };
})();
