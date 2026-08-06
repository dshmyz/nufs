// Bucket quota management — list buckets with live usage, view/set/clear a
// per-bucket quota, and run a quota check against an additional write to
// demonstrate the enforce/deny decision.
(function () {
  'use strict';
  var Vue = window.Vue;
  var U = window.NUFS.Util;
  var A = window.NUFS.API;
  var C = window.NUFS.Components;
  var P = window.NUFS.Pages;

  P.quota = {
    components: { Modal: C.Modal, Empty: C.Empty, CapacityBar: C.CapacityBar },
    data: function () {
      return {
        buckets: [], loading: true, unavailable: false,
        // editor modal state
        edit: null,            // {'name','quota','usage','ratios'} for the selected bucket
        maxBytes: '', maxObjects: '',
        checkResult: null, checkBusy: false, addB: 0, addO: 0,
        busySave: false, busyClear: false, busyCheck: false
      };
    },
    computed: {
      pendingMaxBytes: function () { return this.cleanNum(this.maxBytes); },
      pendingMaxObjects: function () { return this.cleanNum(this.maxObjects); }
    },
    mounted: function () { this.load(); },
    methods: {
      cleanNum: function (v) {
        if (v === '' || v === null || v === undefined) return null;
        var n = parseInt(v, 10);
        return isNaN(n) || n < 0 ? null : n;
      },
      load: function () {
        var self = this;
        this.loading = true; this.unavailable = false;
        A.buckets().then(function (list) {
          // fetch each bucket's quota+usage in parallel
          var detail = (list || []).map(function (b) {
            return A.bucketQuota(b.name).then(function (d) { return d; })
              .catch(function () { return null; });
          });
          return Promise.all(detail).then(function (ds) {
            var out = (list || []).map(function (b, i) {
              var d = ds[i];
              var usage = (d && d.usage) || { used_bytes: 0, objects: 0 };
              var quota = d && d.quota; // {max_bytes,max_objects} | null
              return {
                name: b.name,
                root_inode: b.root_inode,
                policy: b.policy,
                usage: usage,
                quota: quota,
                ratios: (d && d.ratios) || { bytes: 0, objects: 0 }
              };
            });
            self.buckets = out;
            if (!out.length) self.unavailable = true;
          });
        }).catch(function (e) { console.error(e); self.unavailable = true; })
          .finally(function () { self.loading = false; });
      },
      hasQuota: function (b) { return b.quota && (b.quota.max_bytes != null || b.quota.max_objects != null); },
      quotaLabel: function (b) {
        if (!this.hasQuota(b)) return 'unlimited';
        var parts = [];
        if (b.quota.max_bytes != null) parts.push(U.humanBytes(b.quota.max_bytes));
        if (b.quota.max_objects != null) parts.push(b.quota.max_objects + ' objects');
        return parts.join(' / ');
      },
      openEdit: function (b) {
        this.edit = b;
        this.maxBytes = b.quota && b.quota.max_bytes != null ? String(b.quota.max_bytes) : '';
        this.maxObjects = b.quota && b.quota.max_objects != null ? String(b.quota.max_objects) : '';
        this.checkResult = null; this.addB = 0; this.addO = 0;
        this.busySave = false; this.busyClear = false; this.busyCheck = false;
      },
      closeEdit: function () { if (!this.busySave && !this.busyClear && !this.busyCheck) this.edit = null; },
      save: function () {
        var self = this; var b = this.edit; if (!b) return;
        var body = { max_bytes: this.pendingMaxBytes, max_objects: this.pendingMaxObjects };
        this.busySave = true;
        A.setBucketQuota(b.name, body).then(function () {
          C.toast.ok('Set quota for ' + b.name);
          self.busySave = false; self.edit = null; self.load();
        }).catch(function (e) { self.busySave = false; C.toast.err(e.message); });
      },
      clearQuota: function () {
        var self = this; var b = this.edit; if (!b) return;
        this.busyClear = true;
        A.deleteBucketQuota(b.name).then(function () {
          C.toast.ok('Removed quota for ' + b.name);
          self.busyClear = false; self.edit = null; self.load();
        }).catch(function (e) { self.busyClear = false; C.toast.err(e.message); });
      },
      check: function () {
        var self = this; var b = this.edit; if (!b) return;
        var addB = this.cleanNum(this.addB) || 0;
        var addO = this.cleanNum(this.addO) || 0;
        this.busyCheck = true; this.checkResult = null;
        A.checkBucketQuota(b.name, addB, addO).then(function (r) {
          self.checkResult = { ok: true, msg: 'Allowed — fits within quota' };
          C.toast.ok('Quota check passed');
          self.busyCheck = false;
        }).catch(function (e) {
          self.checkResult = { ok: false, msg: e.message };
          C.toast.err(e.message);
          self.busyCheck = false;
        });
      },
      bytePct: function (b) { return b.ratios ? Math.round((b.ratios.bytes || 0) * 100) : 0; },
      fmt: function (ts) { return U.fmtTime(ts, false); }
    },
    template: `
      <div>
        <div class="page-head">
          <h2 class="page-title">Bucket Quota</h2>
          <div class="page-sub">Set per-bucket byte/object limits and watch live usage against them</div>
          <div class="actions-row">
            <button class="btn" @click="load">Refresh</button>
          </div>
        </div>

        <div v-if="loading" class="loading">Loading buckets…</div>

        <div v-else-if="unavailable" class="empty-state">
          <div class="ops-empty-title">No buckets found</div>
          <div class="empty-desc">Quota is enforced per bucket. Create a bucket first, then set a limit here.</div>
        </div>

        <div v-else class="card">
          <div class="card-header"><h3>Buckets ({{ buckets.length }})</h3></div>
          <div class="card-body p-0">
            <table class="table">
              <thead><tr><th>Bucket</th><th>Usage (bytes)</th><th>Quota</th><th>Objects</th><th></th></tr></thead>
              <tbody>
                <tr v-for="b in buckets" :key="b.name">
                  <td>
                    <div class="mono">{{ b.name }}</div>
                    <div class="muted small">inode {{ b.root_inode }} &middot; {{ b.policy || 'policy' }}</div>
                  </td>
                  <td>
                    <div>{{ U.humanBytes(b.usage.used_bytes) }}</div>
                    <CapacityBar v-if="hasQuota(b)" :pct="bytePct(b)"/>
                    <span v-else class="muted small">no limit</span>
                  </td>
                  <td class="mono">{{ quotaLabel(b) }}</td>
                  <td>{{ b.usage.objects }}</td>
                  <td style="text-align:right">
                    <button class="btn btn-sm" @click="openEdit(b)">Edit quota</button>
                  </td>
                </tr>
                <tr v-if="!buckets.length"><td colspan="5" class="empty">No buckets</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        <Modal :show="!!edit" :title="'Quota — ' + (edit ? edit.name : '')" @close="closeEdit">
          <div v-if="edit" class="kv-grid">
            <div class="kv"><div class="k">Used</div><div class="v mono">{{ U.humanBytes(edit.usage.used_bytes) }} / {{ edit.usage.objects }} objects</div></div>
            <div class="kv"><div class="k">Current ratio</div><div class="v mono">{{ bytePct(edit) }}% of bytes limit</div></div>
          </div>
          <div class="form-row">
            <label>Max bytes <span class="muted small">(empty = unlimited)</span></label>
            <input class="input" v-model="maxBytes" placeholder="e.g. 107374182400"/>
          </div>
          <div class="form-row" style="margin-top:8px">
            <label>Max objects <span class="muted small">(empty = unlimited)</span></label>
            <input class="input" v-model="maxObjects" placeholder="e.g. 10000"/>
          </div>

          <div style="border-top:1px solid var(--border);margin-top:12px;padding-top:12px">
            <div class="page-sub">Quota check — will an add of the following fit?</div>
            <div class="kv-grid" style="margin-top:6px">
              <div class="kv"><div class="k">Add bytes</div><div class="v"><input class="input" style="width:100%" v-model.number="addB" placeholder="0"/></div></div>
              <div class="kv"><div class="k">Add objects</div><div class="v"><input class="input" style="width:100%" v-model.number="addO" placeholder="0"/></div></div>
            </div>
            <button class="btn btn-sm" style="margin-top:8px" :disabled="busyCheck" @click="check">{{ busyCheck ? 'Checking…' : 'Check fits' }}</button>
            <div v-if="checkResult" style="margin-top:8px">
              <span class="badge" :class="checkResult.ok ? 'badge-success' : 'badge-danger'">{{ checkResult.msg }}</span>
            </div>
          </div>

          <template #footer>
            <button class="btn btn-sm" :disabled="busySave || busyClear || busyCheck" @click="clearQuota">{{ busyClear ? 'Removing…' : 'Remove quota' }}</button>
            <span style="flex:1"></span>
            <button class="btn" @click="closeEdit" :disabled="busySave || busyClear || busyCheck">Cancel</button>
            <button class="btn btn-primary" @click="save" :disabled="busySave || busyClear || busyCheck">{{ busySave ? 'Saving…' : 'Save quota' }}</button>
          </template>
        </Modal>
        <ToastRoot/>
      </div>
    `
  };
})();
