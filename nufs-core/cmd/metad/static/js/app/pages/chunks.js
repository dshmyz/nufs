// Chunk-level operations — drill from an inode to its chunk list, then into a
// single chunk's metadata and replicas. Actions: commit (checksum), seal,
// migrate a replica to another node, and delete (danger confirm). No verify
// action — the metad exposes no /chunks/<id>/verify route; integrity checks
// run via backing-store verify, not this control plane.
(function () {
  'use strict';
  var Vue = window.Vue;
  var U = window.NUFS.Util;
  var A = window.NUFS.API;
  var C = window.NUFS.Components;
  var P = window.NUFS.Pages;

  function stateBadge(s) {
    s = String(s || '').toLowerCase();
    if (s === 'sealed' || s === 'committed' || s === 'complete') return 'badge-success';
    if (s === 'failed' || s === 'error' || s === 'corrupt') return 'badge-danger';
    if (s === 'writing' || s === 'open' || s === 'allocated') return 'badge-info';
    return 'badge-secondary';
  }
  function replicaState(s) {
    // ReplicaState: 0=syncing,1=ready,2=stale,3=failed
    var map = { 0: 'syncing', 1: 'ready', 2: 'stale', 3: 'failed' };
    return map[s] != null ? map[s] : (map[String(s)] || String(s));
  }

  P.chunks = {
    components: { Modal: C.Modal, Empty: C.Empty },
    props: ['id'],
    data: function () {
      return {
        inodeInput: '',
        refs: null, refsLoading: false, refsError: null,
        detail: null, detailLoading: false, detailId: null,
        nodes: [],
        commitOpen: false, commitChecksum: '', commitBusy: false,
        migrateOpen: false, migrateFrom: '', migrateTo: '', migrateBusy: false,
        deleteOpen: false, deleteBusy: false
      };
    },
    mounted: function () {
      var self = this;
      if (this.$props.id != null && this.$props.id !== '') { this.inodeInput = String(this.$props.id); }
      this.loadNodes();
      if (this.inodeInput) this.loadRefs();
    },
    methods: {
      fmt: function (ts) { return U.fmtTime(ts, true); }, // create_time is Unix ns
      loadNodes: function () {
        var self = this;
        A.nodes().then(function (l) { self.nodes = (l || []).filter(function (n) { return n.state !== 'offline'; }); })
          .catch(function () { self.nodes = []; });
      },
      loadRefs: function () {
        var self = this;
        var inode = String(this.inodeInput || '').trim();
        if (!inode) { C.toast.err('Enter an inode'); return; }
        this.refsLoading = true; this.refsError = null; this.refs = null; this.detail = null; this.detailId = null;
        A.chunksByInode(inode).then(function (r) {
          self.refs = r || [];
        }).catch(function (e) {
          self.refsError = e.message; self.refs = [];
        }).finally(function () { self.refsLoading = false; });
      },
      openDetail: function (chunkId) {
        var self = this;
        this.detailLoading = true; this.detailId = chunkId; this.detail = null;
        A.chunk(chunkId).then(function (r) { self.detail = r; })
          .catch(function (e) { C.toast.err('Failed to load chunk ' + chunkId + ': ' + e.message); })
          .finally(function () { self.detailLoading = false; });
      },
      reloadDetail: function () { if (this.detailId) this.openDetail(this.detailId); },
      // chunk state/ec helpers
      cState: stateBadge,
      cReplicaState: replicaState,
      isEC: function () { return !!(this.detail && this.detail.ec_group); },
      // ---- commit ----
      askCommit: function () { this.commitOpen = true; this.commitChecksum = ''; this.commitBusy = false; },
      cancelCommit: function () { if (!this.commitBusy) { this.commitOpen = false; } },
      doCommit: function () {
        var self = this; var cs = (this.commitChecksum || '').trim();
        if (!this.detailId || cs === '') { C.toast.err('Enter an integer checksum'); return; }
        var num = 0;
        try { num = parseInt(cs, 10); } catch (e) { C.toast.err('Checksum must be an integer'); return; }
        this.commitBusy = true;
        A.chunkCommit(this.detailId, num).then(function () {
          C.toast.ok('Chunk ' + self.detailId + ' committed');
          self.commitBusy = false; self.commitOpen = false; self.reloadDetail();
        }).catch(function (e) { self.commitBusy = false; C.toast.err(e.message); });
      },
      // ---- seal ----
      doSeal: function () {
        var self = this;
        if (!this.detailId) return;
        A.chunkSeal(this.detailId).then(function () {
          C.toast.ok('Chunk ' + self.detailId + ' sealed');
          self.reloadDetail();
        }).catch(function (e) { C.toast.err(e.message); });
      },
      // ---- migrate ----
      askMigrate: function () {
        var self = this;
        this.migrateOpen = true; this.migrateFrom = ''; this.migrateTo = ''; this.migrateBusy = false;
        this.loadNodes();
      },
      cancelMigrate: function () { if (!this.migrateBusy) this.migrateOpen = false; },
      doMigrate: function () {
        var self = this;
        if (!this.detailId) return;
        if (!this.migrateFrom) { C.toast.err('Select source node'); return; }
        if (!this.migrateTo) { C.toast.err('Select destination node'); return; }
        if (this.migrateFrom === this.migrateTo) { C.toast.err('Source and destination must differ'); return; }
        this.migrateBusy = true;
        A.migrateReplica(this.detailId, this.migrateFrom, this.migrateTo).then(function () {
          C.toast.ok('Replica migrated');
          self.migrateBusy = false; self.migrateOpen = false; self.reloadDetail();
        }).catch(function (e) { self.migrateBusy = false; C.toast.err(e.message); });
      },
      // ---- delete ----
      askDelete: function () { this.deleteOpen = true; this.deleteBusy = false; },
      cancelDelete: function () { if (!this.deleteBusy) this.deleteOpen = false; },
      doDelete: function () {
        var self = this;
        if (!this.detailId) return;
        this.deleteBusy = true;
        A.deleteChunk(this.detailId).then(function () {
          C.toast.ok('Chunk ' + self.detailId + ' deleted');
          self.deleteBusy = false; self.deleteOpen = false;
          self.detail = null; self.detailId = null; self.loadRefs();
        }).catch(function (e) { self.deleteBusy = false; C.toast.err(e.message); });
      }
    },
    template: `
      <div>
        <div class="page-head">
          <h2 class="page-title">Chunk Operations</h2>
          <div class="page-sub">Inspect and operate on chunk-level metadata and replicas for an inode</div>
          <div class="actions-row">
            <input class="input" style="width:200px" v-model="inodeInput" placeholder="inode" @keyup.enter="loadRefs"/>
            <button class="btn" :disabled="refsLoading" @click="loadRefs">{{ refsLoading ? 'Loading…' : 'Load chunks' }}</button>
          </div>
        </div>

        <div v-if="refsError" class="ops-empty">
          <div class="ops-empty-title">Could not load chunks</div>
          <div>{{ refsError }}</div>
        </div>

        <div v-else class="card">
          <div class="card-header"><h3>Chunks for inode {{ inodeInput }} ({{ refs ? refs.length : 0 }})</h3></div>
          <div class="card-body p-0">
            <div v-if="refsLoading" class="loading">Loading…</div>
            <table v-else class="table">
              <thead><tr><th>ID</th><th>Offset</th><th>Length</th><th>Version</th><th></th></tr></thead>
              <tbody>
                <tr v-for="r in refs" :key="r.id" :class="{ 'row-active': detailId === r.id }">
                  <td class="mono">{{ r.id }}</td>
                  <td class="mono">{{ r.offset }}</td>
                  <td class="mono">{{ r.length }}</td>
                  <td class="mono">{{ r.version }}</td>
                  <td style="text-align:right">
                    <button class="btn btn-sm" @click="openDetail(r.id)">Inspect</button>
                  </td>
                </tr>
                <tr v-if="!refsLoading && refs && !refs.length"><td colspan="5" class="empty">No chunks for this inode.</td></tr>
                <tr v-if="!refsLoading && !refs"><td colspan="5" class="empty">Enter an inode and load chunks.</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        <!-- detail -->
        <div v-if="detailLoading" class="loading">Loading chunk detail…</div>
        <div v-else-if="detail" class="card">
          <div class="card-header">
            <h3>Chunk {{ detail.id }}</h3>
            <div style="display:flex;gap:8px">
              <button class="btn btn-sm" @click="doSeal">Seal</button>
              <button class="btn btn-sm" @click="askCommit">Commit</button>
              <button class="btn btn-sm" @click="askMigrate">Migrate replica</button>
              <button class="btn btn-sm btn-danger" @click="askDelete">Delete</button>
            </div>
          </div>
          <div class="card-body">
            <div class="kv-grid">
              <div class="kv"><div class="k">State</div><div class="v"><span class="badge" :class="cState(detail.state)">{{ detail.state }}</span> <span v-if="isEC()" class="badge badge-info">EC</span></div></div>
              <div class="kv"><div class="k">Size</div><div class="v mono">{{ detail.size }} bytes</div></div>
              <div class="kv"><div class="k">Tier</div><div class="v mono">{{ detail.tier }}</div></div>
              <div class="kv"><div class="k">Checksum</div><div class="v mono">{{ detail.checksum != null ? detail.checksum : '—' }}</div></div>
              <div class="kv"><div class="k">Created</div><div class="v mono">{{ fmt(detail.create_time) }}</div></div>
              <div class="kv"><div class="k">PG / epoch</div><div class="v mono">{{ detail.pg_id != null ? detail.pg_id + ' / ' + detail.epoch : '—' }}</div></div>
              <div class="kv"><div class="k">Generation</div><div class="v mono">{{ detail.generation || 0 }}</div></div>
            </div>

            <div v-if="isEC() && detail.ec_group" style="margin-top:12px">
              <div class="page-sub">EC group</div>
              <div class="kv-grid">
                <div class="kv"><div class="k">Group ID</div><div class="v mono">{{ detail.ec_group.group_id }}</div></div>
                <div class="kv"><div class="k">Profile</div><div class="v mono">{{ detail.ec_group.profile_id }}</div></div>
                <div class="kv"><div class="k">Data shards</div><div class="v mono">{{ detail.ec_group.data_shards }} + {{ detail.ec_group.parity_shards }}</div></div>
                <div class="kv"><div class="k">Striped</div><div class="v">{{ detail.ec_group.striped ? 'yes' : 'no' }}</div></div>
              </div>
            </div>

            <div style="margin-top:12px">
              <div class="page-sub">Replicas ({{ detail.replicas ? detail.replicas.length : 0 }})</div>
              <table class="table">
                <thead><tr><th>Node</th><th>Address</th><th>State</th><th>Disk</th><th>Shard</th></tr></thead>
                <tbody>
                  <tr v-for="(rp, i) in detail.replicas || []" :key="i">
                    <td class="mono">{{ rp.node_id }}</td>
                    <td class="mono">{{ rp.addr }}</td>
                    <td><span class="badge" :class="cReplicaState(rp.state)">{{ cReplicaState(rp.state) }}</span></td>
                    <td class="mono small">{{ rp.disk_path }}</td>
                    <td class="mono">{{ rp.shard_index }}</td>
                  </tr>
                  <tr v-if="!(detail.replicas || []).length"><td colspan="5" class="empty">No replicas recorded.</td></tr>
                </tbody>
              </table>
            </div>
          </div>
        </div>

        <!-- commit modal -->
        <Modal :show="commitOpen" title="Commit chunk {{ detailId }}" @close="cancelCommit">
          <p style="font-size:.85rem">Record a sealed, integrity-validated checksum (CRC32C as an integer).</p>
          <input class="input" v-model="commitChecksum" placeholder="checksum (integer)" @keyup.enter="doCommit"/>
          <template #footer>
            <button class="btn" @click="cancelCommit" :disabled="commitBusy">Cancel</button>
            <button class="btn btn-primary" @click="doCommit" :disabled="commitBusy">{{ commitBusy ? 'Committing…' : 'Commit' }}</button>
          </template>
        </Modal>

        <!-- migrate modal -->
        <Modal :show="migrateOpen" title="Migrate replica" @close="cancelMigrate">
          <p style="font-size:.85rem">Move this chunk's replica from one node to another.</p>
          <div class="form-row">
            <label>From node</label>
            <select class="input" v-model="migrateFrom">
              <option value="" disabled>select source</option>
              <option v-for="n in nodes" :key="n.id" :value="n.id">{{ n.id }} — {{ n.addr }}</option>
            </select>
          </div>
          <div class="form-row" style="margin-top:8px">
            <label>To node</label>
            <select class="input" v-model="migrateTo">
              <option value="" disabled>select destination</option>
              <option v-for="n in nodes" :key="'t' + n.id" :value="n.id">{{ n.id }} — {{ n.addr }}</option>
            </select>
          </div>
          <template #footer>
            <button class="btn" @click="cancelMigrate" :disabled="migrateBusy">Cancel</button>
            <button class="btn btn-primary" @click="doMigrate" :disabled="migrateBusy">{{ migrateBusy ? 'Migrating…' : 'Migrate' }}</button>
          </template>
        </Modal>

        <!-- delete modal -->
        <Modal :show="deleteOpen" title="Delete chunk {{ detailId }}" @close="cancelDelete">
          <p style="font-size:.9rem">Permanently delete chunk <strong>{{ detailId }}</strong>?<br>
          <span style="color:var(--danger);font-size:.8rem">This removes the chunk metadata and cannot be undone.</span></p>
          <template #footer>
            <button class="btn" @click="cancelDelete" :disabled="deleteBusy">Cancel</button>
            <button class="btn btn-danger" @click="doDelete" :disabled="deleteBusy">{{ deleteBusy ? 'Deleting…' : 'Delete' }}</button>
          </template>
        </Modal>
        <ToastRoot/>
      </div>
    `
  };
})();
