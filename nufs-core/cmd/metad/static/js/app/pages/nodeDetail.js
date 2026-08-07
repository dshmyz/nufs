// Node detail page — health drill-down for a single node.
(function () {
  'use strict';
  var Vue = window.Vue;
  var U = window.NUFS.Util;
  var A = window.NUFS.API;
  var C = window.NUFS.Components;
  var I = window.NUFS.I18n;
  var P = window.NUFS.Pages;

  P.nodeDetail = {
    props: ['id'],
    components: { StateBadge: C.StateBadge, Modal: C.Modal, RingGauge: C.RingGauge },
    data: function () {
      return { node: null, chunks: [], loading: true, notFound: false, busy: false, confirm: false, chunksUnavailable: false, t: I.t };
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
          // The control plane enumerates chunks by inode only — there is no
          // node-scoped "chunks hosted here" listing, so the bare /api/v1/chunks
          // call 400s. Show the hosted count from the node summary and mark the
          // per-chunk breakdown unavailable instead of firing a bad request.
          self.chunks = [];
          self.chunksUnavailable = true;
        }).catch(function (e) { console.error(e); C.toast.err(e.message); })
          .finally(function () { self.loading = false; });
      },
      decommission: function () {
        var self = this;
        this.busy = true;
        A.decommissionNode(this.id).then(function () {
          C.toast.ok(this.t('nd.toast_draining', { id: self.id }));
          self.confirm = false; self.busy = false;
          self.load();
        }.bind(this)).catch(function (e) { self.busy = false; C.toast.err(e.message); });
      },
      back: function () { window.NUFS.Router.navigate('nodes'); }
    },
    template: `
      <div>
        <div class="page-head" v-if="!loading">
          <h2 class="page-title">{{ t('nod.title', { id: id }) }} <StateBadge v-if="node" :state="node.state"/></h2>
          <div class="page-sub"><a href="javascript:void(0)" @click="back">{{ t('nod.back') }}</a></div>
        </div>

        <div v-if="loading" class="loading">{{ t('nod.loading', { id: id }) }}</div>

        <div v-else-if="notFound" class="ops-empty">
          <div class="ops-empty-title">{{ t('nod.notfound', { id: id }) }}</div>
          <div v-html="t('nod.notfound_hint')"></div>
        </div>

        <template v-else-if="node">
          <div class="card">
            <div class="card-header"><h3>{{ t('nod.health') }}</h3>
              <div class="actions-row">
                <button v-if="node.state === 0" class="btn btn-sm btn-danger" @click="confirm = true">{{ t('nd.decommission') }}</button>
              </div>
            </div>
            <div class="card-body">
              <div style="display:flex;align-items:center;gap:16px;margin-bottom:18px">
                <RingGauge :pct="usagePct" :size="56" :color="usagePct > 85 ? 'danger' : (usagePct > 70 ? 'warning' : 'primary')"/>
                <div>
                  <div style="font-size:1.2rem;font-weight:700">{{ usagePct.toFixed(1) }}%</div>
                  <div class="text-muted" style="font-size:.8rem">{{ t('nod.cap_used') }}</div>
                </div>
              </div>
              <div class="kv-grid">
                <div class="kv"><div class="k">{{ t('nod.k_address') }}</div><div class="v mono">{{ node.addr }}</div></div>
                <div class="kv"><div class="k">{{ t('nod.k_rack_zone') }}</div><div class="v">{{ node.rack || '—' }} / {{ node.zone || '—' }}</div></div>
                <div class="kv"><div class="k">{{ t('nod.k_machine') }}</div><div class="v mono">{{ node.machine_id || '—' }}</div></div>
                <div class="kv"><div class="k">{{ t('nod.k_capacity') }}</div><div class="v">{{ U.humanBytes(node.capacity_bytes || node.capacity_gb * 1073741824) }}</div></div>
                <div class="kv"><div class="k">{{ t('nod.k_logical') }}</div><div class="v">{{ U.humanBytes(node.used_bytes || node.used_gb * 1073741824) }}</div></div>
                <div class="kv"><div class="k">{{ t('nod.k_ondisk') }}</div><div class="v" style="color:var(--orange)">{{ U.humanBytes(node.on_disk_bytes || node.on_disk_gb * 1073741824) }}</div></div>
                <div class="kv"><div class="k">{{ t('nod.k_chunks') }}</div><div class="v">{{ node.chunk_count }}</div></div>
                <div class="kv"><div class="k">{{ t('nod.k_lastseen') }}</div><div class="v">{{ U.relTime(node.last_seen, true) }}</div></div>
              </div>
            </div>
          </div>

          <div class="card">
            <div class="card-header"><h3>{{ t('nod.chunks_hosted', { n: node ? node.chunk_count || 0 : 0 }) }}</h3></div>
            <div class="card-body p-0">
              <div v-if="chunksUnavailable" class="empty ops-empty">{{ t('nod.chunks_unavailable') }}</div>
              <table v-else class="table">
                <thead><tr><th>{{ t('nod.th_chunk') }}</th><th>{{ t('nod.th_size') }}</th><th>{{ t('nod.th_state') }}</th><th>{{ t('nod.th_replicas') }}</th></tr></thead>
                <tbody>
                  <tr v-for="c in chunks" :key="c.id">
                    <td class="mono">{{ c.id }}</td>
                    <td>{{ U.humanBytes(c.size) }}</td>
                    <td class="text-muted">{{ U.stateLabel(c.state) }}</td>
                    <td>{{ (c.replicas || []).length }}</td>
                  </tr>
                  <tr v-if="!chunks.length"><td colspan="4" class="empty">{{ t('nod.no_chunks') }}</td></tr>
                </tbody>
              </table>
            </div>
          </div>

          <Modal :show="confirm" :title="t('nd.modal_title')" @close="confirm = false">
            <p style="font-size:.9rem" v-html="t('nod.decommission_modal', { id: id })"></p>
            <template #footer>
              <button class="btn" @click="confirm = false" :disabled="busy">{{ t('common.cancel') }}</button>
              <button class="btn btn-danger" @click="decommission" :disabled="busy">{{ busy ? t('nd.draining_btn') : t('nd.decommission') }}</button>
            </template>
          </Modal>
        </template>
        <ToastRoot/>
      </div>
    `
  };
})();
