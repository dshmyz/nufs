// Nodes page — health list + per-node decommission action.
(function () {
  'use strict';
  var Vue = window.Vue;
  var U = window.NUFS.Util;
  var A = window.NUFS.API;
  var C = window.NUFS.Components;
  var I = window.NUFS.I18n;
  var P = window.NUFS.Pages;

  P.nodes = {
    components: { StateBadge: C.StateBadge, Modal: C.Modal, Empty: C.Empty },
    data: function () {
      return {
        nodes: [], loading: true,
        confirm: null, // node being decommissioned, or null
        busy: false,
        t: I.t
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
          C.toast.ok(self.t('nd.toast_draining', { id: n.id }));
          self.confirm = null; self.busy = false;
          self.load();
        }).catch(function (e) { self.busy = false; C.toast.err(e.message); });
      },
      go: function (id) { window.NUFS.Router.navigate('node_detail', { id: id }); },
      restore: function (n) {
        var self = this;
        this.busy = true;
        A.restoreNode(n.id).then(function () {
          C.toast.ok(self.t('nd.toast_restored', { id: n.id }));
          self.busy = false;
          self.load();
        }).catch(function (e) { self.busy = false; C.toast.err(e.message); });
      },
      usagePct: function (n) { return n.capacity_gb > 0 ? ((n.used_gb / n.capacity_gb) * 100) : 0; }
    },
    template: `
      <div>
        <div class="page-head">
          <h2 class="page-title">{{ t('nd.title') }}</h2>
          <div class="page-sub">{{ t('nd.sub', { n: nodes.length }) }}</div>
        </div>

        <div v-if="loading" class="loading">{{ t('nd.loading') }}</div>

        <div v-else class="card">
          <div class="card-body p-0">
            <table class="table">
              <thead><tr>
                <th data-sort="int">ID</th><th data-sort="string">{{ t('nd.th_address') }}</th><th data-sort="string">{{ t('nd.th_state') }}</th>
                <th data-sort="string">{{ t('nd.th_rack') }}</th><th data-sort="string">{{ t('nd.th_zone') }}</th>
                <th data-sort="int">{{ t('nd.th_capacity') }}</th><th data-sort="int">{{ t('nd.th_used') }}</th><th data-sort="int">{{ t('nd.th_ondisk') }}</th>
                <th data-sort="int">{{ t('nd.th_chunks') }}</th><th data-sort="string">{{ t('nd.th_lastseen') }}</th><th></th>
              </tr></thead>
              <tbody>
                <tr v-for="n in nodes" :key="n.id" class="clickable-row" @click="go(n.id)">
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
                    <button v-if="n.state === 0" class="btn btn-sm btn-danger" @click.stop="askDecommission(n)">{{ t('nd.decommission') }}</button>
                    <span v-else-if="n.state === 1 || n.state === 2" class="text-muted" style="font-size:.75rem">{{ t('nd.draining_lbl') }}</span>
                    <span v-else-if="n.state === 5" class="text-muted" style="font-size:.75rem">{{ t('nd.decommissioned_lbl') }}</span>
                    <button v-if="n.state !== 0" class="btn btn-sm btn-primary" style="margin-left:6px" @click.stop="restore(n)">{{ t('nd.restore') }}</button>
                  </td>
                </tr>
                <tr v-if="!nodes.length"><td colspan="11" class="empty">{{ t('nd.no_nodes') }}</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        <Modal :show="!!confirm" :title="t('nd.modal_title')" @close="cancelConfirm">
          <p style="font-size:.9rem" v-html="t('nd.modal_body', { id: confirm ? confirm.id : '', addr: confirm ? confirm.addr : '' })"></p>
          <template #footer>
            <button class="btn" @click="cancelConfirm" :disabled="busy">{{ t('common.cancel') }}</button>
            <button class="btn btn-danger" @click="confirmDecommission" :disabled="busy">{{ busy ? t('nd.draining_btn') : t('nd.decommission') }}</button>
          </template>
        </Modal>
        <ToastRoot/>
      </div>
    `
  };
})();
