// Node detail page — health drill-down for a single node.
(function () {
  'use strict';
  var Vue = window.Vue;
  var U = window.NUFS.Util;
  var A = window.NUFS.API;
  var C = window.NUFS.Components;
  var P = window.NUFS.Pages;

  P.nodeDetail = {
    props: ['id'],
    components: { StateBadge: C.StateBadge, Modal: C.Modal, RingGauge: C.RingGauge },
    data: function () {
      return { node: null, chunks: [], loading: true, notFound: false, busy: false, confirm: false };
    },
    computed: {
      usagePct: function () {
        if (!this.node) return 0;
        return this.node.capacity_gb > 0 ? ((this.node.used_gb / this.node.capacity_gb) * 100) : 0;
      }
    },
    watch: { id: function () { this.load(); } },
    mounted: function () { this.load(); },
    methods: {
      load: function () {
        var self = this;
        this.loading = true; this.notFound = false;
        A.node(this.id).then(function (n) {
          if (!n) { self.notFound = true; return; }
          self.node = n;
          return A.chunks().then(function (ch) {
            self.chunks = (ch || []).filter(function (c) {
              return (c.replicas || []).some(function (r) { return r.node_id === n.id || r.node_id === Number(self.id); });
            });
          });
        }).catch(function (e) { console.error(e); C.toast.err(e.message); })
          .finally(function () { self.loading = false; });
      },
      decommission: function () {
        var self = this;
        this.busy = true;
        A.decommissionNode(this.id).then(function () {
          C.toast.ok('Node ' + self.id + ' is draining');
          self.confirm = false; self.busy = false;
          self.load();
        }).catch(function (e) { self.busy = false; C.toast.err(e.message); });
      },
      back: function () { window.NUFS.Router.navigate('nodes'); }
    },
    template: `
      <div>
        <div class="page-head" v-if="!loading">
          <h2 class="page-title">Node {{ id }} <StateBadge v-if="node" :state="node.state"/></h2>
          <div class="page-sub"><a href="javascript:void(0)" @click="back">&larr; Back to nodes</a></div>
        </div>

        <div v-if="loading" class="loading">Loading node {{ id }}…</div>

        <div v-else-if="notFound" class="ops-empty">
          <div class="ops-empty-title">Node {{ id }} not found</div>
          <div>It may not be registered. <a href="javascript:void(0)" @click="back">Back to nodes</a></div>
        </div>

        <template v-else-if="node">
          <div class="card">
            <div class="card-header"><h3>Health</h3>
              <div class="actions-row">
                <button v-if="node.state === 0" class="btn btn-sm btn-danger" @click="confirm = true">Decommission</button>
              </div>
            </div>
            <div class="card-body">
              <div style="display:flex;align-items:center;gap:16px;margin-bottom:18px">
                <RingGauge :pct="usagePct" :size="56" :color="usagePct > 85 ? 'danger' : (usagePct > 70 ? 'warning' : 'primary')"/>
                <div>
                  <div style="font-size:1.2rem;font-weight:700">{{ usagePct.toFixed(1) }}%</div>
                  <div class="text-muted" style="font-size:.8rem">capacity used</div>
                </div>
              </div>
              <div class="kv-grid">
                <div class="kv"><div class="k">Address</div><div class="v mono">{{ node.addr }}</div></div>
                <div class="kv"><div class="k">Rack / zone</div><div class="v">{{ node.rack || '—' }} / {{ node.zone || '—' }}</div></div>
                <div class="kv"><div class="k">Machine</div><div class="v mono">{{ node.machine_id || '—' }}</div></div>
                <div class="kv"><div class="k">Capacity</div><div class="v">{{ U.humanBytes(node.capacity_bytes || node.capacity_gb * 1073741824) }}</div></div>
                <div class="kv"><div class="k">Logical used</div><div class="v">{{ U.humanBytes(node.used_bytes || node.used_gb * 1073741824) }}</div></div>
                <div class="kv"><div class="k">On disk</div><div class="v" style="color:var(--orange)">{{ U.humanBytes(node.on_disk_bytes || node.on_disk_gb * 1073741824) }}</div></div>
                <div class="kv"><div class="k">Chunks</div><div class="v">{{ node.chunk_count }}</div></div>
                <div class="kv"><div class="k">Last seen</div><div class="v">{{ U.relTime(node.last_seen, true) }}</div></div>
              </div>
            </div>
          </div>

          <div class="card">
            <div class="card-header"><h3>Chunks hosted ({{ chunks.length }})</h3></div>
            <div class="card-body p-0">
              <table class="table">
                <thead><tr><th>Chunk</th><th>Size</th><th>State</th><th>Replicas</th></tr></thead>
                <tbody>
                  <tr v-for="c in chunks" :key="c.id">
                    <td class="mono">{{ c.id }}</td>
                    <td>{{ U.humanBytes(c.size) }}</td>
                    <td class="text-muted">{{ c.state }}</td>
                    <td>{{ (c.replicas || []).length }}</td>
                  </tr>
                  <tr v-if="!chunks.length"><td colspan="4" class="empty">No chunks on this node</td></tr>
                </tbody>
              </table>
            </div>
          </div>

          <Modal :show="confirm" title="Decommission node" @close="confirm = false">
            <p style="font-size:.9rem">Drain and decommission <strong>node {{ id }}</strong>? Its replicas will migrate before it goes offline.</p>
            <template #footer>
              <button class="btn" @click="confirm = false" :disabled="busy">Cancel</button>
              <button class="btn btn-danger" @click="decommission" :disabled="busy">{{ busy ? 'Draining…' : 'Decommission' }}</button>
            </template>
          </Modal>
        </template>
        <ToastRoot/>
      </div>
    `
  };
})();
