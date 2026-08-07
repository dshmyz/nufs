// Backup management — list committed backups + tasks, run a new backup,
// verify an artifact, and preview prune candidates.
(function () {
  'use strict';
  var Vue = window.Vue;
  var U = window.NUFS.Util;
  var A = window.NUFS.API;
  var C = window.NUFS.Components;
  var I = window.NUFS.I18n;
  var P = window.NUFS.Pages;

  function asArray(l) { return (l || []).slice().sort(function (a, b) { return (b.created_at || '').localeCompare(a.created_at || ''); }); }

  P.backups = {
    components: { Empty: C.Empty },
    data: function () {
      return {
        list: null, status: null, loading: true, unavailable: false,
        busyCreate: false, busyId: null, pruneBusy: false, pruneResult: null,
        pollTimer: null, t: I.t
      };
    },
    computed: {
      tasks: function () { return this.list ? asArray(this.list.tasks) : []; },
      catalogBackups: function () {
        if (this.list && this.list.catalog && this.list.catalog.backups) return asArray(this.list.catalog.backups);
        if (this.status && this.status.catalog && this.status.catalog.backups) return asArray(this.status.catalog.backups);
        return [];
      },
      subText: function () {
        if (!this.status) return '';
        return this.status.Active ? this.t('bk.sub_active', { ret: this.status.Retention }) : this.t('bk.sub_idle', { ret: this.status.Retention });
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
          C.toast.ok(this.t('bk.toast_started'));
          self.busyCreate = false;
          self.load();
        }.bind(this)).catch(function (e) { self.busyCreate = false; C.toast.err(e.message); });
      },
      verify: function (id) {
        var self = this;
        this.busyId = id;
        A.verifyBackup(id).then(function (r) {
          C.toast.ok(this.t(r && r.verified ? 'bk.toast_verified' : 'bk.toast_verified2', { id: id }));
          self.busyId = null;
        }.bind(this)).catch(function (e) { self.busyId = null; C.toast.err(e.message); });
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
      },
      stateLabel: function (s) { return U.stateLabel(s); }
    },
    template: `
      <div>
        <div class="page-head">
          <h2 class="page-title">{{ t('bk.title') }}</h2>
          <div class="page-sub" v-if="status">{{ subText }}</div>
          <div class="actions-row">
            <button class="btn" @click="load()">{{ t('bk.refresh') }}</button>
            <button class="btn btn-primary" :disabled="busyCreate || (status && status.Active)" @click="createBackup">{{ busyCreate ? t('bk.starting') : t('bk.run') }}</button>
          </div>
        </div>

        <div v-if="loading" class="loading">{{ t('bk.loading') }}</div>

        <div v-else-if="unavailable" class="empty-state">
          <div class="ops-empty-title">{{ t('bk.unconfigured') }}</div>
          <div class="empty-desc">{{ t('bk.unconfigured_hint') }}</div>
        </div>

        <template v-else>
          <div v-if="status" class="card">
            <div class="card-header"><h3>{{ t('bk.coordinator') }}</h3></div>
            <div class="card-body">
              <div class="kv-grid">
                <div class="kv"><div class="k">{{ t('bk.k_last') }}</div><div class="v mono">{{ status.LastBackupID || '—' }}</div></div>
                <div class="kv"><div class="k">{{ t('bk.k_scheduled') }}</div><div class="v">{{ rel(status.NextRunAt) }}</div></div>
                <div class="kv"><div class="k">{{ t('bk.k_started') }}</div><div class="v">{{ fmt(status.LastStartedAt) }}</div></div>
                <div class="kv"><div class="k">{{ t('bk.k_error') }}</div><div class="v" style="color:var(--red)">{{ status.LastError || '—' }}</div></div>
              </div>
            </div>
          </div>

          <div v-if="pruneResult" class="card">
            <div class="card-header"><h3>{{ t('bk.prune_preview') }}</h3>
              <span class="badge badge-info">{{ t('bk.prune_badge', { committed: pruneResult.committed_backups, ret: pruneResult.retention }) }}</span>
            </div>
            <div class="card-body">
              <p v-if="!(pruneResult.deletion_candidates || []).length" class="empty">{{ t('bk.prune_none') }}</p>
              <table v-else class="table">
                <thead><tr><th>{{ t('bk.th_pid') }}</th><th>{{ t('bk.th_created') }}</th><th>{{ t('bk.th_bytes') }}</th></tr></thead>
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
            <div class="card-header"><h3>{{ t('bk.catalog', { n: catalogBackups.length }) }}</h3>
              <button class="btn btn-sm" :disabled="pruneBusy" @click="previewPrune">{{ pruneBusy ? '…' : t('bk.preview_prune') }}</button>
            </div>
            <div class="card-body p-0">
              <table class="table">
                <thead><tr><th>{{ t('bk.th_pid') }}</th><th>{{ t('bk.th_created') }}</th><th>{{ t('bk.th_bytes') }}</th><th>{{ t('bk.th_term') }}</th><th></th></tr></thead>
                <tbody>
                  <tr v-for="b in catalogBackups" :key="b.id">
                    <td class="mono">{{ b.id }}</td>
                    <td>{{ fmt(b.created_at) }}</td>
                    <td>{{ U.humanBytes(b.total_bytes) }}</td>
                    <td>{{ b.raft_term }}</td>
                    <td style="text-align:right">
                      <button class="btn btn-sm" :disabled="busyId === b.id" @click="verify(b.id)">{{ busyId === b.id ? t('bk.verifying') : t('bk.verify') }}</button>
                    </td>
                  </tr>
                  <tr v-if="!catalogBackups.length"><td colspan="5" class="empty">{{ t('bk.no_catalog') }}</td></tr>
                </tbody>
              </table>
            </div>
          </div>

          <div class="card">
            <div class="card-header"><h3>{{ t('bk.runs', { n: tasks.length }) }}</h3></div>
            <div class="card-body p-0">
              <table class="table">
                <thead><tr><th>{{ t('bk.th_pid') }}</th><th>{{ t('bk.th_state') }}</th><th>{{ t('bk.th_started') }}</th><th>{{ t('bk.th_bytes') }}</th><th>{{ t('bk.th_files') }}</th></tr></thead>
                <tbody>
                  <tr v-for="t in tasks" :key="t.id">
                    <td class="mono">{{ t.id }}</td>
                    <td><span class="badge" :class="stateBadge(t.state)">{{ stateLabel(t.state) }}</span></td>
                    <td>{{ fmt(t.started_at) }}</td>
                    <td>{{ U.humanBytes(t.bytes_uploaded) }}</td>
                    <td>{{ t.files_uploaded || 0 }}</td>
                  </tr>
                  <tr v-if="!tasks.length"><td colspan="5" class="empty">{{ t('bk.no_runs') }}</td></tr>
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
