// Nodes page — health list + per-node decommission action.
(function () {
  'use strict';
  var Vue = window.Vue;
  var U = window.NUFS.Util;
  var A = window.NUFS.API;
  var C = window.NUFS.Components;
  var P = window.NUFS.Pages;

  P.nodes = {
    components: { StateBadge: C.StateBadge, Modal: C.Modal, Empty: C.Empty },
    data: function () {
      return {
        nodes: [], loading: true,
        confirm: null, // node being decommissioned, or null
        busy: false
      };
    },
    mounted: function () { this.load(); },
    methods: {
      load: function () {
        var self = this;
        this.loading = true;
        A.nodes().then(function (d) { self.nodes = d || []; })
          .catch(function (e) { console.error(e); C.toast.err(e.message); })
          .finally(function () { self.loading = false; });
      },
      askDecommission: function (n) { this.confirm = n; },
      cancelConfirm: function () { if (!this.busy) this.confirm = null; },
      confirmDecommission: function () {
        var self = this;
        var n = this.confirm;
        if (!n) return;
        this.busy = true;
        A.decommissionNode(n.id).then(function () {
          C.toast.ok('Node ' + n.id + ' is draining');
          self.confirm = null; self.busy = false;
          self.load();
        }).catch(function (e) { self.busy = false; C.toast.err(e.message); });
      },
      go: function (id) { window.NUFS.Router.navigate('node_detail', { id: id }); },
      usagePct: function (n) { return n.capacity_gb > 0 ? ((n.used_gb / n.capacity_gb) * 100) : 0; }
    },
    template: `
      <div>
        <div class="page-head">
          <h2 class="page-title">Nodes</h2>
          <div class="page-sub">{{ nodes.length }} registered peer{{ nodes.length !== 1 ? 's' : '' }}</div>
        </div>

        <div v-if="loading" class="loading">Loading nodes…</div>

        <div v-else class="card">
          <div class="card-body p-0">
            <table class="table">
              <thead><tr>
                <th data-sort="int">ID</th><th data-sort="string">Address</th><th data-sort="string">State</th>
                <th data-sort="string">Rack</th><th data-sort="string">Zone</th>
                <th data-sort="int">Capacity</th><th data-sort="int">Used</th><th data-sort="int">On disk</th>
                <th data-sort="int">Chunks</th><th data-sort="string">Last seen</th><th></th>
              </tr></thead>
              <tbody>
                <tr v-for="n in nodes" :key="n.id">
                  <td><a href="javascript:void(0)" @click="go(n.id)">{{ n.id }}</a></td>
                  <td class="mono">{{ n.addr }}</td>
                  <td><StateBadge :state="n.state"/></td>
                  <td>{{ n.rack || '—' }}</td>
                  <td>{{ n.zone || '—' }}</td>
                  <td>{{ U.humanBytes(n.capacity_bytes || n.capacity_gb * 1073741824) }}</td>
                  <td>{{ U.humanBytes(n.used_bytes || n.used_gb * 1073741824) }}</td>
                  <td class="on-disk">{{ U.humanBytes(n.on_disk_bytes || n.on_disk_gb * 1073741824) }}</td>
                  <td>{{ n.chunk_count }}</td>
                  <td class="text-muted">{{ U.relTime(n.last_seen, true) }}</td>
                  <td>
                    <button v-if="n.state !== 1 && n.state !== 2 && n.state !== 4" class="btn btn-sm btn-danger" @click="askDecommission(n)">Decommission</button>
                    <span v-else class="text-muted" style="font-size:.75rem">draining</span>
                  </td>
                </tr>
                <tr v-if="!nodes.length"><td colspan="11" class="empty">No nodes registered</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        <Modal :show="!!confirm" title="Decommission node" @close="cancelConfirm">
          <p style="font-size:.9rem">Drain and decommission <strong>node {{ confirm ? confirm.id : '' }}</strong> ({{ confirm ? confirm.addr : '' }})?<br>
          <span class="text-muted">Its replicas will migrate to surviving peers before it goes offline.</span></p>
          <template #footer>
            <button class="btn" @click="cancelConfirm" :disabled="busy">Cancel</button>
            <button class="btn btn-danger" @click="confirmDecommission" :disabled="busy">{{ busy ? 'Draining…' : 'Decommission' }}</button>
          </template>
        </Modal>
        <ToastRoot/>
      </div>
    `
  };
})();
