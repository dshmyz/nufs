// Single-chunk detail — routed from #/chunks/<id> (a chunk id, not an inode).
// Renders the chunk's metadata (state/size/tier/checksum/pg/generation), its
// EC group layout, the replica distribution table, and the control actions
// commit / seal / migrate / delete. This is the extracted detail body that
// used to live inline in chunks.js; the chunks list page now drills here.
(function () {
  'use strict';
  var Vue = window.Vue;
  var U = window.NUFS.Util;
  var A = window.NUFS.API;
  var C = window.NUFS.Components;
  var I = window.NUFS.I18n;
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

  P.chunkDetail = {
    components: { Modal: C.Modal },
    props: ['id'],
    data: function () {
      return {
        detail: null, detailLoading: false, notFound: false,
        nodes: [],
        commitOpen: false, commitChecksum: '', commitBusy: false,
        migrateOpen: false, migrateFrom: '', migrateTo: '', migrateBusy: false,
        deleteOpen: false, deleteBusy: false,
        t: I.t
      };
    },
    watch: { id: function () { this.load(); } },
    mounted: function () { this.load(); },
    methods: {
      fmt: function (ts) { return U.fmtTime(ts, true); }, // create_time is Unix ns
      back: function () { window.NUFS.Router.navigate('chunks'); },
      load: function () {
        var self = this;
        var chunkId = String(this.$props.id != null ? this.$props.id : '').trim();
        if (!chunkId) { this.notFound = true; return; }
        this.loadNodes();
        this.detailLoading = true; this.notFound = false; this.detail = null;
        A.chunk(chunkId).then(function (r) { self.detail = r; })
          .catch(function (e) { console.error(e); self.notFound = true; })
          .finally(function () { self.detailLoading = false; });
      },
      reload: function () { var id = this.detail && this.detail.id; if (id) this.load(); },
      loadNodes: function () {
        var self = this;
        A.nodes().then(function (l) { self.nodes = (l || []).filter(function (n) { return n.state !== 'offline'; }); })
          .catch(function () { self.nodes = []; });
      },
      // chunk state/ec helpers
      cState: stateBadge,
      cReplicaState: replicaState,
      isEC: function () { return !!(this.detail && this.detail.ec_group); },
      // ---- commit ----
      askCommit: function () { this.commitOpen = true; this.commitChecksum = ''; this.commitBusy = false; },
      cancelCommit: function () { if (!this.commitBusy) { this.commitOpen = false; } },
      doCommit: function () {
        var self = this; var id = this.detail.id; var cs = (this.commitChecksum || '').trim();
        if (cs === '') { C.toast.err(this.t('ck.toast_checksum')); return; }
        var num = 0;
        try { num = parseInt(cs, 10); } catch (e) { C.toast.err(this.t('ck.toast_checksum')); return; }
        this.commitBusy = true;
        A.chunkCommit(id, num).then(function () {
          C.toast.ok(this.t('ck.toast_committed', { id: id }));
          self.commitBusy = false; self.commitOpen = false; self.reload();
        }.bind(this)).catch(function (e) { self.commitBusy = false; C.toast.err(e.message); });
      },
      // ---- seal ----
      doSeal: function () {
        var self = this; var id = this.detail.id;
        A.chunkSeal(id).then(function () {
          C.toast.ok(self.t('ck.toast_sealed', { id: id }));
          self.reload();
        }).catch(function (e) { C.toast.err(e.message); });
      },
      // ---- migrate ----
      askMigrate: function () {
        this.migrateOpen = true; this.migrateFrom = ''; this.migrateTo = ''; this.migrateBusy = false;
        this.loadNodes();
      },
      cancelMigrate: function () { if (!this.migrateBusy) this.migrateOpen = false; },
      doMigrate: function () {
        var self = this; var id = this.detail.id;
        if (!this.migrateFrom) { C.toast.err(this.t('ck.toast_sel_from')); return; }
        if (!this.migrateTo) { C.toast.err(this.t('ck.toast_sel_to')); return; }
        if (this.migrateFrom === this.migrateTo) { C.toast.err(this.t('ck.toast_same')); return; }
        this.migrateBusy = true;
        A.migrateReplica(id, this.migrateFrom, this.migrateTo).then(function () {
          C.toast.ok(this.t('ck.toast_migrated'));
          self.migrateBusy = false; self.migrateOpen = false; self.reload();
        }.bind(this)).catch(function (e) { self.migrateBusy = false; C.toast.err(e.message); });
      },
      // ---- delete ----
      askDelete: function () { this.deleteOpen = true; this.deleteBusy = false; },
      cancelDelete: function () { if (!this.deleteBusy) this.deleteOpen = false; },
      doDelete: function () {
        var self = this; var id = this.detail.id;
        this.deleteBusy = true;
        A.deleteChunk(id).then(function () {
          C.toast.ok(this.t('ck.toast_deleted', { id: id }));
          self.deleteBusy = false; self.deleteOpen = false;
          self.back();
        }.bind(this)).catch(function (e) { self.deleteBusy = false; C.toast.err(e.message); });
      }
    },
    template: `
      <div>
        <div class="page-head">
          <h2 class="page-title">{{ t('page.chunk_detail') }}</h2>
          <div class="page-sub"><a href="javascript:void(0)" @click="back">{{ t('cd.back') }}</a></div>
        </div>

        <div v-if="detailLoading" class="loading">{{ t('ck.loading_detail') }}</div>

        <div v-else-if="notFound || !detail" class="ops-empty">
          <div class="ops-empty-title">{{ t('cd.notfound', { id: id }) }}</div>
          <div v-html="t('cd.notfound_hint')"></div>
        </div>

        <div v-else class="card">
          <div class="card-header">
            <h3>{{ t('ck.title') }} {{ detail.id }}</h3>
            <div style="display:flex;gap:8px">
              <button class="btn btn-sm" @click="doSeal">{{ t('ck.seal') }}</button>
              <button class="btn btn-sm" @click="askCommit">{{ t('ck.commit') }}</button>
              <button class="btn btn-sm" @click="askMigrate">{{ t('ck.migrate') }}</button>
              <button class="btn btn-sm btn-danger" @click="askDelete">{{ t('ck.delete') }}</button>
            </div>
          </div>
          <div class="card-body">
            <div class="kv-grid">
              <div class="kv"><div class="k">{{ t('ck.k_state') }}</div><div class="v"><span class="badge" :class="cState(detail.state)">{{ U.stateLabel(detail.state) }}</span> <span v-if="isEC()" class="badge badge-info">EC</span></div></div>
              <div class="kv"><div class="k">{{ t('ck.k_size') }}</div><div class="v mono">{{ t('ck.bytes', { n: detail.size }) }}</div></div>
              <div class="kv"><div class="k">{{ t('ck.k_tier') }}</div><div class="v mono">{{ detail.tier }}</div></div>
              <div class="kv"><div class="k">{{ t('ck.k_checksum') }}</div><div class="v mono">{{ detail.checksum != null ? detail.checksum : '—' }}</div></div>
              <div class="kv"><div class="k">{{ t('ck.k_created') }}</div><div class="v mono">{{ fmt(detail.create_time) }}</div></div>
              <div class="kv"><div class="k">{{ t('ck.k_pg') }}</div><div class="v mono">{{ detail.pg_id != null ? detail.pg_id + ' / ' + detail.epoch : '—' }}</div></div>
              <div class="kv"><div class="k">{{ t('ck.k_generation') }}</div><div class="v mono">{{ detail.generation || 0 }}</div></div>
            </div>

            <div v-if="isEC() && detail.ec_group" style="margin-top:12px">
              <div class="page-sub">{{ t('ck.ec_group') }}</div>
              <div class="kv-grid">
                <div class="kv"><div class="k">{{ t('ck.ec_group_id') }}</div><div class="v mono">{{ detail.ec_group.group_id }}</div></div>
                <div class="kv"><div class="k">{{ t('ck.ec_profile') }}</div><div class="v mono">{{ detail.ec_group.profile_id }}</div></div>
                <div class="kv"><div class="k">{{ t('ck.ec_shards') }}</div><div class="v mono">{{ detail.ec_group.data_shards }} + {{ detail.ec_group.parity_shards }}</div></div>
                <div class="kv"><div class="k">{{ t('ck.ec_striped') }}</div><div class="v">{{ detail.ec_group.striped ? t('ck.yes') : t('ck.no') }}</div></div>
              </div>
            </div>

            <div style="margin-top:12px">
              <div class="page-sub">{{ t('ck.replicas', { n: detail.replicas ? detail.replicas.length : 0 }) }}</div>
              <table class="table">
                <thead><tr><th>{{ t('ck.rp_node') }}</th><th>{{ t('ck.rp_addr') }}</th><th>{{ t('ck.rp_state') }}</th><th>{{ t('ck.rp_disk') }}</th><th>{{ t('ck.rp_shard') }}</th></tr></thead>
                <tbody>
                  <tr v-for="(rp, i) in detail.replicas || []" :key="i">
                    <td class="mono">{{ rp.node_id }}</td>
                    <td class="mono">{{ rp.addr }}</td>
                    <td><span class="badge" :class="cReplicaState(rp.state)">{{ cReplicaState(rp.state) }}</span></td>
                    <td class="mono small">{{ rp.disk_path }}</td>
                    <td class="mono">{{ rp.shard_index }}</td>
                  </tr>
                  <tr v-if="!(detail.replicas || []).length"><td colspan="5" class="empty">{{ t('ck.rp_none') }}</td></tr>
                </tbody>
              </table>
            </div>
          </div>
        </div>

        <!-- commit modal -->
        <Modal :show="commitOpen" :title="t('ck.modal_commit', { id: id })" @close="cancelCommit">
          <p style="font-size:.85rem">{{ t('ck.commit_desc') }}</p>
          <input class="input" v-model="commitChecksum" :placeholder="t('ck.ph_checksum')" @keyup.enter="doCommit"/>
          <template #footer>
            <button class="btn" @click="cancelCommit" :disabled="commitBusy">{{ t('common.cancel') }}</button>
            <button class="btn btn-primary" @click="doCommit" :disabled="commitBusy">{{ commitBusy ? t('ck.committing') : t('ck.commit') }}</button>
          </template>
        </Modal>

        <!-- migrate modal -->
        <Modal :show="migrateOpen" :title="t('ck.modal_migrate')" @close="cancelMigrate">
          <p style="font-size:.85rem">{{ t('ck.migrate_desc') }}</p>
          <div class="form-row">
            <label>{{ t('ck.from_node') }}</label>
            <select class="input" v-model="migrateFrom">
              <option value="" disabled>{{ t('ck.sel_src') }}</option>
              <option v-for="n in nodes" :key="n.id" :value="n.id">{{ n.id }} — {{ n.addr }}</option>
            </select>
          </div>
          <div class="form-row" style="margin-top:8px">
            <label>{{ t('ck.to_node') }}</label>
            <select class="input" v-model="migrateTo">
              <option value="" disabled>{{ t('ck.sel_dst') }}</option>
              <option v-for="n in nodes" :key="'t' + n.id" :value="n.id">{{ n.id }} — {{ n.addr }}</option>
            </select>
          </div>
          <template #footer>
            <button class="btn" @click="cancelMigrate" :disabled="migrateBusy">{{ t('common.cancel') }}</button>
            <button class="btn btn-primary" @click="doMigrate" :disabled="migrateBusy">{{ migrateBusy ? t('ck.migrating') : t('ck.migrate') }}</button>
          </template>
        </Modal>

        <!-- delete modal -->
        <Modal :show="deleteOpen" :title="t('ck.modal_delete', { id: id })" @close="cancelDelete">
          <p style="font-size:.9rem" v-html="t('ck.delete_desc', { id: id })"></p>
          <template #footer>
            <button class="btn" @click="cancelDelete" :disabled="deleteBusy">{{ t('common.cancel') }}</button>
            <button class="btn btn-danger" @click="doDelete" :disabled="deleteBusy">{{ deleteBusy ? t('ck.deleting') : t('ck.delete') }}</button>
          </template>
        </Modal>
        <ToastRoot/>
      </div>
    `
  };
})();
