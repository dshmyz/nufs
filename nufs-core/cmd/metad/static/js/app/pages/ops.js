// Data-ops observability — one page aggregating the maintenance/diagnostic
// surfaces: scrub, write-ops status + write attempts by state, the audit
// log, and advisory locks. Each section loads independently and tolerates a
// 503/unconfigured subsystem (audit) without taking the others down.
(function () {
  'use strict';
  var Vue = window.Vue;
  var U = window.NUFS.Util;
  var A = window.NUFS.API;
  var C = window.NUFS.Components;
  var I = window.NUFS.I18n;
  var P = window.NUFS.Pages;

  var STATES = ['pending', 'chunks_allocated', 'chunks_durable', 'committed', 'failed', 'recovery_needed'];

  function stateBadge(s) {
    s = String(s || '').toLowerCase();
    if (s === 'committed' || s === 'done' || s === 'complete') return 'badge-success';
    if (s === 'failed' || s === 'error') return 'badge-danger';
    if (s === 'recovery_needed' || s === 'chunks_durable') return 'badge-warning';
    if (s === 'pending' || s === 'chunks_allocated' || s === 'running' || s === 'queued') return 'badge-info';
    return 'badge-secondary';
  }

  P.ops = {
    components: { Empty: C.Empty },
    data: function () {
      return {
        // scrub
        scrub: null, scrubLoading: true, scrubBusy: false,
        // write-ops
        os: null, osLoading: true, osUnavailable: false,
        // write-attempts
        attempts: null, attemptsLoading: false, attemptsState: 'pending',
        // audit
        audit: null, auditLoading: true, auditUnavailable: false,
        // locks
        locks: null, locksLoading: false, lockInode: '',
        t: I.t
      };
    },
    computed: {
      attemptStates: function () { return STATES; },
      osCounts: function () {
        if (!this.os || !this.os.attempts) return [];
        return STATES.map(function (s) { return { state: s, count: this.os.attempts[s] || 0 }; }.bind(this));
      },
      // task may be an empty object {} when no background task is defined
      recoveryTask: function () {
        var t = this.os && this.os.recovery_task;
        return t && t.id ? t : null;
      },
      gcTask: function () {
        var t = this.os && this.os.gc_task;
        return t && t.id ? t : null;
      }
    },
    mounted: function () {
      this.loadScrub(); this.loadOpsStatus(); this.loadAudit(); this.loadAttempts();
    },
    methods: {
      // Most timestamp fields here are Unix nanoseconds (audit ts, background
      // task updated_at, write-attempt CreatedAt, lock since_unix_nano).
      fmt: function (ts) { return U.fmtTime(ts, true); },
      stateBadge: stateBadge,
      // Scrub returns `timestamp` as an RFC3339 string (not unix), so format
      // string timestamps separately; coerce numeric unix-nano to Date too.
      fmtStamp: function (ts) {
        if (ts == null) return '-';
        if (typeof ts === 'string') { var d = new Date(ts); return isNaN(d.getTime()) ? ts : d.toLocaleString(); }
        return U.fmtTime(ts, true);
      },
      loadScrub: function () {
        var self = this;
        this.scrubLoading = true;
        A.get('/api/v1/scrub').then(function (r) { self.scrub = r; })
          .catch(function (e) { console.error(e); self.scrub = null; })
          .finally(function () { self.scrubLoading = false; });
      },
      runScrub: function () {
        var self = this;
        this.scrubBusy = true;
        A.scrub().then(function (r) {
          C.toast.ok(this.t('ops.toast_scrub'));
          self.scrub = r;
          self.scrubBusy = false;
        }.bind(this)).catch(function (e) { self.scrubBusy = false; C.toast.err(e.message); });
      },

      // ---- write-ops ----
      loadOpsStatus: function () {
        var self = this;
        this.osLoading = true; this.osUnavailable = false;
        A.writeOpsStatus().then(function (r) {
          self.os = r;
          if (!r || !r.attempts) self.osUnavailable = true;
        }).catch(function (e) { console.error(e); self.os = null; self.osUnavailable = true; })
          .finally(function () { self.osLoading = false; });
      },

      // ---- write-attempts ----
      loadAttempts: function () {
        var self = this;
        this.attemptsLoading = true; this.attempts = null;
        A.writeAttempts(this.attemptsState).then(function (r) { self.attempts = r || []; })
          .catch(function (e) { console.error(e); self.attempts = []; C.toast.err(self.t('ops.toast_load_attempts')); })
          .finally(function () { self.attemptsLoading = false; });
      },

      // ---- audit ----
      loadAudit: function () {
        var self = this;
        this.auditLoading = true; this.auditUnavailable = false; this.audit = null;
        A.audit({ limit: 100 }).then(function (r) { self.audit = r || []; })
          .catch(function (e) {
            // 503 = audit logger not enabled; treat as unavailable, not error
            self.audit = null; self.auditUnavailable = true;
          })
          .finally(function () { self.auditLoading = false; });
      },

      // ---- locks ----
      loadLocks: function () {
        var self = this;
        var inode = String(this.lockInode || '').trim();
        if (!inode) { C.toast.err(self.t('ops.toast_inode')); return; }
        this.locksLoading = true; this.locks = null;
        A.locks(inode).then(function (r) { self.locks = r || []; })
          .catch(function (e) { self.locks = []; C.toast.err(e.message); })
          .finally(function () { self.locksLoading = false; });
      }
    },
    template: `
      <div>
        <div class="page-head">
          <h2 class="page-title">{{ t('ops.title') }}</h2>
          <div class="page-sub">{{ t('ops.sub') }}</div>
          <div class="actions-row">
            <button class="btn" @click="loadScrub; loadOpsStatus; loadAudit; loadAttempts">{{ t('ops.refresh_all') }}</button>
          </div>
        </div>

        <!-- Scrub -->
        <div class="card">
          <div class="card-header"><h3>{{ t('ops.scrub') }}</h3>
            <button class="btn btn-sm" :disabled="scrubBusy" @click="runScrub">{{ scrubBusy ? t('ops.scrubbing') : t('ops.run_scrub') }}</button>
          </div>
          <div class="card-body">
            <div v-if="scrubLoading" class="loading">{{ t('ops.scrub_loading') }}</div>
            <div v-else-if="!scrub" class="empty">{{ t('ops.scrub_none') }}</div>
            <div v-else class="kv-grid">
              <div class="kv"><div class="k">{{ t('ops.k_scanned') }}</div><div class="v mono">{{ scrub.scanned }}</div></div>
              <div class="kv"><div class="k">{{ t('ops.k_healthy') }}</div><div class="v mono" style="color:var(--green)">{{ scrub.healthy }}</div></div>
              <div class="kv"><div class="k">{{ t('ops.k_unhealthy') }}</div><div class="v mono" style="color:var(--red)">{{ scrub.unhealthy }}</div></div>
              <div class="kv"><div class="k">{{ t('ops.k_duration') }}</div><div class="v mono">{{ scrub.duration }}</div></div>
              <div class="kv"><div class="k">{{ t('ops.k_ran') }}</div><div class="v mono">{{ fmtStamp(scrub.timestamp) }}</div></div>
              <div class="kv"><div class="k">{{ t('ops.k_health') }}</div><div class="v">
                <span class="badge" :class="(scrub.unhealthy||0)>0 ? 'badge-danger' : 'badge-success'">{{ (scrub.unhealthy||0)>0 ? t('ops.badge_degraded') : t('ops.badge_healthy') }}</span>
              </div></div>
            </div>
          </div>
        </div>

        <!-- Write ops status -->
        <div class="card">
          <div class="card-header"><h3>{{ t('ops.wr_pipeline') }}</h3>
            <button class="btn btn-sm" @click="loadOpsStatus">{{ t('ops.refresh') }}</button>
          </div>
          <div class="card-body">
            <div v-if="osLoading" class="loading">{{ t('common.loading') }}</div>
            <div v-else-if="osUnavailable || !os" class="empty">{{ t('ops.wr_unavailable') }}</div>
            <template v-else>
              <div class="stat-chips">
                <div v-for="c in osCounts" :key="c.state" class="stat-chip">
                  <div class="stat-chip-num mono">{{ c.count }}</div>
                  <div class="stat-chip-label">{{ c.state }}</div>
                </div>
              </div>

              <div class="card" style="margin-top:12px">
                <div class="card-header"><h3 style="font-size:.9rem">{{ t('ops.bg_tasks') }}</h3></div>
                <div class="card-body">
                  <div v-if="recoveryTask || gcTask" class="kv-grid">
                    <div v-if="recoveryTask" class="kv" style="grid-column:span 1">
                      <div class="k">{{ t('ops.recovery') }} ({{ recoveryTask.type }})</div>
                      <div class="v">
                        <span class="badge" :class="stateBadge(recoveryTask.state)">{{ U.stateLabel(recoveryTask.state) }}</span>
                        <div class="muted small">{{ t('ops.task_meta', { a: recoveryTask.attempt_count, t: fmt(recoveryTask.updated_at) }) }}</div>
                        <div v-if="recoveryTask.target" class="muted small mono">{{ t('ops.target_lbl') }} {{ recoveryTask.target }}</div>
                        <div v-if="recoveryTask.last_error" class="small" style="color:var(--red)">{{ recoveryTask.last_error }}</div>
                      </div>
                    </div>
                    <div v-if="gcTask" class="kv" style="grid-column:span 1">
                      <div class="k">{{ t('ops.gc') }} ({{ gcTask.type }})</div>
                      <div class="v">
                        <span class="badge" :class="stateBadge(gcTask.state)">{{ U.stateLabel(gcTask.state) }}</span>
                        <div class="muted small">{{ t('ops.task_meta', { a: gcTask.attempt_count, t: fmt(gcTask.updated_at) }) }}</div>
                        <div v-if="gcTask.target" class="muted small mono">{{ t('ops.target_lbl') }} {{ gcTask.target }}</div>
                        <div v-if="gcTask.last_error" class="small" style="color:var(--red)">{{ gcTask.last_error }}</div>
                      </div>
                    </div>
                  </div>
                  <div v-else class="empty">{{ t('ops.no_bg') }}</div>
                </div>
              </div>
            </template>
          </div>
        </div>

        <!-- Write attempts -->
        <div class="card">
          <div class="card-header"><h3>{{ t('ops.wr_attempts') }}</h3>
            <div style="display:flex;gap:8px;align-items:center">
              <select class="input" style="width:auto" v-model="attemptsState" @change="loadAttempts">
                <option v-for="s in attemptStates" :key="s" :value="s">{{ s }}</option>
              </select>
              <button class="btn btn-sm" @click="loadAttempts">{{ attemptsLoading ? '…' : t('ops.refresh') }}</button>
            </div>
          </div>
          <div class="card-body p-0">
            <div v-if="attemptsLoading" class="loading">{{ t('common.loading') }}</div>
            <table v-else class="table">
              <thead><tr><th>{{ t('ops.th_id') }}</th><th>{{ t('ops.th_bucket') }}</th><th>{{ t('ops.th_inode') }}</th><th>{{ t('ops.th_state') }}</th><th>{{ t('ops.th_err') }}</th><th>{{ t('ops.th_created') }}</th></tr></thead>
              <tbody>
                <tr v-for="a in attempts" :key="a.ID || a.id">
                  <td class="mono">{{ a.ID }}</td>
                  <td><span class="mono">{{ a.Bucket }}</span>/<span class="mono">{{ a.Key }}</span></td>
                  <td class="mono">{{ a.InodeID }}</td>
                  <td><span class="badge" :class="stateBadge(a.State || a.state)">{{ a.State || a.state }}</span></td>
                  <td class="muted small">{{ a.LastError || a.last_error || '—' }}</td>
                  <td class="mono small">{{ fmt(a.CreatedAt || a.created_at) }}</td>
                </tr>
                <tr v-if="!attemptsLoading && (!attempts || !attempts.length)"><td colspan="6" class="empty">{{ t('ops.attempts_empty', { state: attemptsState }) }}</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        <!-- Audit -->
        <div class="card">
          <div class="card-header"><h3>{{ t('ops.audit', { n: audit ? audit.length : 0 }) }}</h3>
            <button class="btn btn-sm" @click="loadAudit">{{ t('ops.refresh') }}</button>
          </div>
          <div class="card-body p-0">
            <div v-if="auditLoading" class="loading">{{ t('common.loading') }}</div>
            <div v-else-if="auditUnavailable" class="empty-state">
              <div class="ops-empty-title">{{ t('ops.audit_not_enabled') }}</div>
              <div class="empty-desc">{{ t('ops.audit_not_enabled_hint') }}</div>
            </div>
            <table v-else class="table">
              <thead><tr><th>{{ t('ops.th_time') }}</th><th>{{ t('ops.th_action') }}</th><th>{{ t('ops.th_actor') }}</th><th>{{ t('ops.th_resource') }}</th><th>{{ t('ops.th_result') }}</th></tr></thead>
              <tbody>
                <tr v-for="r in audit" :key="r.id || r.ts">
                  <td class="mono small">{{ fmt(r.ts) }}</td>
                  <td class="mono">{{ r.action }}</td>
                  <td class="mono">{{ r.actor }}</td>
                  <td class="mono">{{ r.resource }}</td>
                  <td><span class="badge" :class="r.result === 'ok' ? 'badge-success' : 'badge-danger'">{{ r.result }}</span></td>
                </tr>
                <tr v-if="!audit || !audit.length"><td colspan="5" class="empty">{{ t('ops.audit_empty') }}</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        <!-- Locks -->
        <div class="card">
          <div class="card-header"><h3>{{ t('ops.locks') }}</h3>
            <div style="display:flex;gap:8px;align-items:center">
              <input class="input" style="width:140px" v-model="lockInode" :placeholder="t('ops.ph_inode')" @keyup.enter="loadLocks"/>
              <button class="btn btn-sm" :disabled="locksLoading" @click="loadLocks">{{ locksLoading ? '…' : t('ops.inspect') }}</button>
            </div>
          </div>
          <div class="card-body p-0">
            <p class="muted small" style="padding:8px 12px;margin:0">{{ t('ops.locks_hint') }}</p>
            <table class="table">
              <thead><tr><th>{{ t('ops.th_l_inode') }}</th><th>{{ t('ops.th_owner') }}</th><th>{{ t('ops.th_mode') }}</th><th>{{ t('ops.th_since') }}</th></tr></thead>
              <tbody>
                <tr v-for="(l, i) in locks || []" :key="i">
                  <td class="mono">{{ l.inode }}</td>
                  <td class="mono">{{ l.owner }}</td>
                  <td><span class="badge" :class="l.mode === 'exclusive' ? 'badge-warning' : 'badge-info'">{{ l.mode }}</span></td>
                  <td class="mono small">{{ fmt(l.since_unix_nano) }}</td>
                </tr>
                <tr v-if="locks && !locks.length"><td colspan="4" class="empty">{{ t('ops.locks_empty') }}</td></tr>
              </tbody>
            </table>
          </div>
        </div>
        <ToastRoot/>
      </div>
    `
  };
})();
