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
        locks: null, locksLoading: false, lockInode: ''
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
          C.toast.ok('Scrub completed');
          self.scrub = r;
          self.scrubBusy = false;
        }).catch(function (e) { self.scrubBusy = false; C.toast.err(e.message); });
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
          .catch(function (e) { console.error(e); self.attempts = []; C.toast.err('Failed to load attempts'); })
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
        if (!inode) { C.toast.err('Enter an inode to inspect locks'); return; }
        this.locksLoading = true; this.locks = null;
        A.locks(inode).then(function (r) { self.locks = r || []; })
          .catch(function (e) { self.locks = []; C.toast.err(e.message); })
          .finally(function () { self.locksLoading = false; });
      }
    },
    template: `
      <div>
        <div class="page-head">
          <h2 class="page-title">Data Ops Observability</h2>
          <div class="page-sub">Scrub, write pipeline health, write attempts, audit log, and advisory locks</div>
          <div class="actions-row">
            <button class="btn" @click="loadScrub; loadOpsStatus; loadAudit; loadAttempts">Refresh all</button>
          </div>
        </div>

        <!-- Scrub -->
        <div class="card">
          <div class="card-header"><h3>Scrub</h3>
            <button class="btn btn-sm" :disabled="scrubBusy" @click="runScrub">{{ scrubBusy ? 'Scrubbing…' : 'Run scrub' }}</button>
          </div>
          <div class="card-body">
            <div v-if="scrubLoading" class="loading">Loading…</div>
            <div v-else-if="!scrub" class="empty">No scrub result yet — run a scrub.</div>
            <div v-else class="kv-grid">
              <div class="kv"><div class="k">Scanned</div><div class="v mono">{{ scrub.scanned }}</div></div>
              <div class="kv"><div class="k">Healthy</div><div class="v mono" style="color:var(--green)">{{ scrub.healthy }}</div></div>
              <div class="kv"><div class="k">Unhealthy</div><div class="v mono" style="color:var(--red)">{{ scrub.unhealthy }}</div></div>
              <div class="kv"><div class="k">Duration</div><div class="v mono">{{ scrub.duration }}</div></div>
              <div class="kv"><div class="k">Ran at</div><div class="v mono">{{ fmtStamp(scrub.timestamp) }}</div></div>
              <div class="kv"><div class="k">Health</div><div class="v">
                <span class="badge" :class="(scrub.unhealthy||0)>0 ? 'badge-danger' : 'badge-success'">{{ (scrub.unhealthy||0)>0 ? 'degraded' : 'all healthy' }}</span>
              </div></div>
            </div>
          </div>
        </div>

        <!-- Write ops status -->
        <div class="card">
          <div class="card-header"><h3>Write pipeline</h3>
            <button class="btn btn-sm" @click="loadOpsStatus">Refresh</button>
          </div>
          <div class="card-body">
            <div v-if="osLoading" class="loading">Loading…</div>
            <div v-else-if="osUnavailable || !os" class="empty">Write-ops status unavailable on this process.</div>
            <template v-else>
              <div class="stat-chips">
                <div v-for="c in osCounts" :key="c.state" class="stat-chip">
                  <div class="stat-chip-num mono">{{ c.count }}</div>
                  <div class="stat-chip-label">{{ c.state }}</div>
                </div>
              </div>

              <div class="card" style="margin-top:12px">
                <div class="card-header"><h3 style="font-size:.9rem">Background tasks</h3></div>
                <div class="card-body">
                  <div v-if="recoveryTask || gcTask" class="kv-grid">
                    <div v-if="recoveryTask" class="kv" style="grid-column:span 1">
                      <div class="k">Recovery ({{ recoveryTask.type }})</div>
                      <div class="v">
                        <span class="badge" :class="stateBadge(recoveryTask.state)">{{ recoveryTask.state }}</span>
                        <div class="muted small">attempts {{ recoveryTask.attempt_count }} &middot; updated {{ fmt(recoveryTask.updated_at) }}</div>
                        <div v-if="recoveryTask.target" class="muted small mono">target {{ recoveryTask.target }}</div>
                        <div v-if="recoveryTask.last_error" class="small" style="color:var(--red)">{{ recoveryTask.last_error }}</div>
                      </div>
                    </div>
                    <div v-if="gcTask" class="kv" style="grid-column:span 1">
                      <div class="k">GC ({{ gcTask.type }})</div>
                      <div class="v">
                        <span class="badge" :class="stateBadge(gcTask.state)">{{ gcTask.state }}</span>
                        <div class="muted small">attempts {{ gcTask.attempt_count }} &middot; updated {{ fmt(gcTask.updated_at) }}</div>
                        <div v-if="gcTask.target" class="muted small mono">target {{ gcTask.target }}</div>
                        <div v-if="gcTask.last_error" class="small" style="color:var(--red)">{{ gcTask.last_error }}</div>
                      </div>
                    </div>
                  </div>
                  <div v-else class="empty">No background tasks registered.</div>
                </div>
              </div>
            </template>
          </div>
        </div>

        <!-- Write attempts -->
        <div class="card">
          <div class="card-header"><h3>Write attempts</h3>
            <div style="display:flex;gap:8px;align-items:center">
              <select class="input" style="width:auto" v-model="attemptsState" @change="loadAttempts">
                <option v-for="s in attemptStates" :key="s" :value="s">{{ s }}</option>
              </select>
              <button class="btn btn-sm" @click="loadAttempts">{{ attemptsLoading ? '…' : 'Refresh' }}</button>
            </div>
          </div>
          <div class="card-body p-0">
            <div v-if="attemptsLoading" class="loading">Loading…</div>
            <table v-else class="table">
              <thead><tr><th>ID</th><th>Bucket / Key</th><th>Inode</th><th>State</th><th>Last error</th><th>Created</th></tr></thead>
              <tbody>
                <tr v-for="a in attempts" :key="a.ID || a.id">
                  <td class="mono">{{ a.ID }}</td>
                  <td><span class="mono">{{ a.Bucket }}</span>/<span class="mono">{{ a.Key }}</span></td>
                  <td class="mono">{{ a.InodeID }}</td>
                  <td><span class="badge" :class="stateBadge(a.State || a.state)">{{ a.State || a.state }}</span></td>
                  <td class="muted small">{{ a.LastError || a.last_error || '—' }}</td>
                  <td class="mono small">{{ fmt(a.CreatedAt || a.created_at) }}</td>
                </tr>
                <tr v-if="!attemptsLoading && (!attempts || !attempts.length)"><td colspan="6" class="empty">No attempts in state "{{ attemptsState }}".</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        <!-- Audit -->
        <div class="card">
          <div class="card-header"><h3>Audit log ({{ audit ? audit.length : 0 }})</h3>
            <button class="btn btn-sm" @click="loadAudit">Refresh</button>
          </div>
          <div class="card-body p-0">
            <div v-if="auditLoading" class="loading">Loading…</div>
            <div v-else-if="auditUnavailable" class="empty-state">
              <div class="ops-empty-title">Audit logger not enabled</div>
              <div class="empty-desc">The audit endpoint returned 503. Enable the audit logger on the metad process to record access events.</div>
            </div>
            <table v-else class="table">
              <thead><tr><th>Time</th><th>Action</th><th>Actor</th><th>Resource</th><th>Result</th></tr></thead>
              <tbody>
                <tr v-for="r in audit" :key="r.id || r.ts">
                  <td class="mono small">{{ fmt(r.ts) }}</td>
                  <td class="mono">{{ r.action }}</td>
                  <td class="mono">{{ r.actor }}</td>
                  <td class="mono">{{ r.resource }}</td>
                  <td><span class="badge" :class="r.result === 'ok' ? 'badge-success' : 'badge-danger'">{{ r.result }}</span></td>
                </tr>
                <tr v-if="!audit || !audit.length"><td colspan="5" class="empty">No audit records yet.</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        <!-- Locks -->
        <div class="card">
          <div class="card-header"><h3>Advisory locks</h3>
            <div style="display:flex;gap:8px;align-items:center">
              <input class="input" style="width:140px" v-model="lockInode" placeholder="inode" @keyup.enter="loadLocks"/>
              <button class="btn btn-sm" :disabled="locksLoading" @click="loadLocks">{{ locksLoading ? '…' : 'Inspect' }}</button>
            </div>
          </div>
          <div class="card-body p-0">
            <p class="muted small" style="padding:8px 12px;margin:0">Enter an inode to list current lock holders on it.</p>
            <table class="table">
              <thead><tr><th>Inode</th><th>Owner</th><th>Mode</th><th>Since</th></tr></thead>
              <tbody>
                <tr v-for="(l, i) in locks || []" :key="i">
                  <td class="mono">{{ l.inode }}</td>
                  <td class="mono">{{ l.owner }}</td>
                  <td><span class="badge" :class="l.mode === 'exclusive' ? 'badge-warning' : 'badge-info'">{{ l.mode }}</span></td>
                  <td class="mono small">{{ fmt(l.since_unix_nano) }}</td>
                </tr>
                <tr v-if="locks && !locks.length"><td colspan="4" class="empty">No locks held on this inode.</td></tr>
              </tbody>
            </table>
          </div>
        </div>
        <ToastRoot/>
      </div>
    `
  };
})();
