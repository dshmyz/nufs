// Backup management — list committed backups + tasks, run a new backup,
// verify an artifact, and preview prune candidates.
(function () {
  'use strict';
  var Vue = window.Vue;
  var U = window.NUFS.Util;
  var A = window.NUFS.API;
  var C = window.NUFS.Components;
  var P = window.NUFS.Pages;

  function asArray(l) { return (l || []).slice().sort(function (a, b) { return (b.created_at || '').localeCompare(a.created_at || ''); }); }

  P.backups = {
    components: { Empty: C.Empty },
    data: function () {
      return {
        list: null, status: null, loading: true, unavailable: false,
        busyCreate: false, busyId: null, pruneBusy: false, pruneResult: null,
        pollTimer: null
      };
    },
    computed: {
      tasks: function () { return this.list ? asArray(this.list.tasks) : []; },
      catalogBackups: function () {
        if (this.list && this.list.catalog && this.list.catalog.backups) return asArray(this.list.catalog.backups);
        if (this.status && this.status.catalog && this.status.catalog.backups) return asArray(this.status.catalog.backups);
        return [];
      }
    },
    mounted: function () {
      this.load();
      var self = this;
      this.pollTimer = setInterval(function () { self.load(true); }, 8000);
    },
    beforeUnmount: function () { if (this.pollTimer) clearInterval(this.pollTimer); },
    methods: {
      load: function (silent) {
        var self = this;
        if (!silent) this.loading = true;
        this.unavailable = false;
        Promise.all([
          A.backups().catch(function () { return null; }),
          A.backupStatus().catch(function () { return null; })
        ]).then(function (r) {
          self.list = r[0]; self.status = r[1];
          if (!self.list && !self.status) self.unavailable = true;
        }).finally(function () { self.loading = false; });
      },
      createBackup: function () {
        var self = this;
        this.busyCreate = true;
        A.createBackup().then(function () {
          C.toast.ok('Backup started');
          self.busyCreate = false;
          self.load();
        }).catch(function (e) { self.busyCreate = false; C.toast.err(e.message); });
      },
      verify: function (id) {
        var self = this;
        this.busyId = id;
        A.verifyBackup(id).then(function (r) {
          C.toast.ok('Backup ' + id + (r && r.verified ? ' verified OK' : ' verified'));
          self.busyId = null;
        }).catch(function (e) { self.busyId = null; C.toast.err(e.message); });
      },
      previewPrune: function () {
        var self = this;
        this.pruneBusy = true; this.pruneResult = null;
        A.pruneBackups().then(function (r) { self.pruneResult = r; self.pruneBusy = false; })
          .catch(function (e) { self.pruneBusy = false; C.toast.err(e.message); });
      },
      fmt: function (ts) { return U.fmtTime(ts, false); },
      rel: function (ts) { return U.relTime(ts, false); },
      stateBadge: function (s) {
        s = String(s || '').toLowerCase();
        if (s === 'done' || s === 'committed') return 'badge-success';
        if (s === 'failed' || s === 'error') return 'badge-danger';
        if (s === 'creating' || s === 'uploading' || s === 'verifying') return 'badge-warning';
        return 'badge-secondary';
      }
    },
    template: `
      <div>
        <div class="page-head">
          <h2 class="page-title">Backups</h2>
          <div class="page-sub" v-if="status">Retention {{ status.Retention }} &middot; {{ status.Active ? 'backup in progress' : 'idle' }}</div>
          <div class="actions-row">
            <button class="btn" @click="load()">Refresh</button>
            <button class="btn btn-primary" :disabled="busyCreate || (status && status.Active)" @click="createBackup">{{ busyCreate ? 'Starting…' : 'Run backup' }}</button>
          </div>
        </div>

        <div v-if="loading" class="loading">Loading backup status…</div>

        <div v-else-if="unavailable" class="empty-state">
          <div class="ops-empty-title">Backup coordinator not configured</div>
          <div class="empty-desc">Backups are disabled on this cluster. Configure the backup coordinator to enable catalog + backups.</div>
        </div>

        <template v-else>
          <div v-if="status" class="card">
            <div class="card-header"><h3>Coordinator status</h3></div>
            <div class="card-body">
              <div class="kv-grid">
                <div class="kv"><div class="k">Last backup</div><div class="v mono">{{ status.LastBackupID || '—' }}</div></div>
                <div class="kv"><div class="k">Scheduled</div><div class="v">{{ rel(status.NextRunAt) }}</div></div>
                <div class="kv"><div class="k">Last started</div><div class="v">{{ fmt(status.LastStartedAt) }}</div></div>
                <div class="kv"><div class="k">Last error</div><div class="v" style="color:var(--red)">{{ status.LastError || '—' }}</div></div>
              </div>
            </div>
          </div>

          <div v-if="pruneResult" class="card">
            <div class="card-header"><h3>Prune preview (dry run)</h3>
              <span class="badge badge-info">{{ pruneResult.committed_backups }} committed, retention {{ pruneResult.retention }}</span>
            </div>
            <div class="card-body">
              <p v-if="!(pruneResult.deletion_candidates || []).length" class="empty">Nothing to prune — within retention.</p>
              <table v-else class="table">
                <thead><tr><th>ID</th><th>Created</th><th>Bytes</th></tr></thead>
                <tbody>
                  <tr v-for="b in pruneResult.deletion_candidates" :key="b.id">
                    <td class="mono">{{ b.id }}</td>
                    <td>{{ fmt(b.created_at) }}</td>
                    <td>{{ U.humanBytes(b.total_bytes) }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>

          <div class="card">
            <div class="card-header"><h3>Committed catalog ({{ catalogBackups.length }})</h3>
              <button class="btn btn-sm" :disabled="pruneBusy" @click="previewPrune">{{ pruneBusy ? '…' : 'Preview prune' }}</button>
            </div>
            <div class="card-body p-0">
              <table class="table">
                <thead><tr><th>ID</th><th>Created</th><th>Bytes</th><th>Term</th><th></th></tr></thead>
                <tbody>
                  <tr v-for="b in catalogBackups" :key="b.id">
                    <td class="mono">{{ b.id }}</td>
                    <td>{{ fmt(b.created_at) }}</td>
                    <td>{{ U.humanBytes(b.total_bytes) }}</td>
                    <td>{{ b.raft_term }}</td>
                    <td style="text-align:right">
                      <button class="btn btn-sm" :disabled="busyId === b.id" @click="verify(b.id)">{{ busyId === b.id ? 'Verifying…' : 'Verify' }}</button>
                    </td>
                  </tr>
                  <tr v-if="!catalogBackups.length"><td colspan="5" class="empty">No committed backups yet</td></tr>
                </tbody>
              </table>
            </div>
          </div>

          <div class="card">
            <div class="card-header"><h3>Runs ({{ tasks.length }})</h3></div>
            <div class="card-body p-0">
              <table class="table">
                <thead><tr><th>ID</th><th>State</th><th>Started</th><th>Bytes</th><th>Files</th></tr></thead>
                <tbody>
                  <tr v-for="t in tasks" :key="t.id">
                    <td class="mono">{{ t.id }}</td>
                    <td><span class="badge" :class="stateBadge(t.state)">{{ t.state }}</span></td>
                    <td>{{ fmt(t.started_at) }}</td>
                    <td>{{ U.humanBytes(t.bytes_uploaded) }}</td>
                    <td>{{ t.files_uploaded || 0 }}</td>
                  </tr>
                  <tr v-if="!tasks.length"><td colspan="5" class="empty">No backup runs yet</td></tr>
                </tbody>
              </table>
            </div>
          </div>
        </template>
        <ToastRoot/>
      </div>
    `
  };
})();
