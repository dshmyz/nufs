// Repair console — the pending anti-entropy / repair queue plus a trigger action.
(function () {
  'use strict';
  var Vue = window.Vue;
  var U = window.NUFS.Util;
  var A = window.NUFS.API;
  var C = window.NUFS.Components;
  var P = window.NUFS.Pages;

  P.repair = {
    components: { Empty: C.Empty },
    data: function () {
      return { tasks: [], loading: true, busy: false, pollTimer: null };
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
        A.repairQueue().then(function (d) { self.tasks = d || []; })
          .catch(function (e) { console.error(e); if (!silent) C.toast.err(e.message); })
          .finally(function () { self.loading = false; });
      },
      trigger: function () {
        var self = this;
        this.busy = true;
        A.triggerRepair().then(function () {
          C.toast.ok('Repair pass triggered');
          self.busy = false;
          self.load();
        }).catch(function (e) { self.busy = false; C.toast.err(e.message); });
      },
      relTime: function (ts) { return U.relTime(ts, false); },
      priorityLabel: function (p) {
        if (p >= 3) return 'high';
        if (p === 2) return 'med';
        if (p === 1) return 'low';
        return '—';
      }
    },
    template: `
      <div>
        <div class="page-head">
          <h2 class="page-title">Repair Queue</h2>
          <div class="page-sub">{{ tasks.length }} task{{ tasks.length !== 1 ? 's' : '' }} waiting for anti-entropy / repair</div>
          <div class="actions-row">
            <button class="btn" @click="load()">Refresh</button>
            <button class="btn btn-primary" :disabled="busy || tasks.length === 0" @click="trigger">{{ busy ? 'Triggering…' : 'Trigger repair pass' }}</button>
          </div>
        </div>

        <div v-if="loading" class="loading">Loading repair queue…</div>

        <div v-else class="card">
          <div class="card-body p-0">
            <table class="table">
              <thead><tr><th>Chunk</th><th>Reason</th><th>Priority</th><th>Queued</th></tr></thead>
              <tbody>
                <tr v-for="t in tasks" :key="t.chunk_id">
                  <td class="mono">{{ t.chunk_id }}</td>
                  <td>{{ t.reason || '—' }}</td>
                  <td><span class="badge" :class="priorityLabel(t.priority) === 'high' ? 'badge-danger' : (priorityLabel(t.priority) === 'med' ? 'badge-warning' : 'badge-secondary')">{{ t.priority }}</span></td>
                  <td class="text-muted">{{ relTime(t.created_at) }}</td>
                </tr>
                <tr v-if="!tasks.length"><td colspan="4" class="empty">No pending repairs — the cluster is healthy.</td></tr>
              </tbody>
            </table>
          </div>
        </div>
        <ToastRoot/>
      </div>
    `
  };
})();
