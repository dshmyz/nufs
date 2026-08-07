// Rebalance console — cluster balance posture + a manual rebalance trigger.
(function () {
  'use strict';
  var Vue = window.Vue;
  var U = window.NUFS.Util;
  var A = window.NUFS.API;
  var C = window.NUFS.Components;
  var I = window.NUFS.I18n;
  var P = window.NUFS.Pages;

  P.rebalance = {
    components: { Empty: C.Empty },
    data: function () {
      return { balance: null, loading: true, busy: false, t: I.t };
    },
    mounted: function () { this.load(); },
    methods: {
      load: function () {
        var self = this;
        this.loading = true;
        A.clusterBalance().then(function (d) { self.balance = d; })
          .catch(function (e) { console.error(e); C.toast.err(e.message); })
          .finally(function () { self.loading = false; });
      },
      trigger: function () {
        var self = this;
        this.busy = true;
        A.triggerRebalance().then(function () {
          C.toast.ok(this.t('rb.toast_triggered'));
          self.busy = false;
          self.load();
        }.bind(this)).catch(function (e) { self.busy = false; C.toast.err(e.message); });
      },
      spreadClass: function () {
        var b = this.balance;
        if (!b) return 'secondary';
        if (b.imbalance < 0.10) return 'success';
        if (b.imbalance < 0.25) return 'info';
        if (b.imbalance < 0.50) return 'warning';
        return 'danger';
      }
    },
    template: `
      <div>
        <div class="page-head">
          <h2 class="page-title">{{ t('rb.title') }}</h2>
          <div class="page-sub">{{ t('rb.sub') }}</div>
          <div class="actions-row">
            <button class="btn" @click="load()">{{ t('rb.refresh') }}</button>
            <button class="btn btn-primary" :disabled="busy" @click="trigger">{{ busy ? t('rb.triggering') : t('rb.trigger') }}</button>
          </div>
        </div>

        <div v-if="loading" class="loading">{{ t('rb.loading') }}</div>

        <template v-else-if="balance">
          <div class="card">
            <div class="card-header"><h3>{{ t('rb.spread') }}</h3>
              <span class="badge" :class="'badge-' + spreadClass()">{{ (balance.imbalance * 100).toFixed(1) }}%</span>
            </div>
            <div class="card-body">
              <div class="kv-grid">
                <div class="kv"><div class="k">{{ t('rb.k_nodes') }}</div><div class="v">{{ (balance.nodes || []).length }}</div></div>
                <div class="kv"><div class="k">{{ t('rb.k_used') }}</div><div class="v">{{ (balance.total_used_pct * 100).toFixed(1) }}%</div></div>
                <div class="kv"><div class="k">{{ t('rb.k_reco') }}</div><div class="v">{{ balance.recommendation || '—' }}</div></div>
              </div>
              <p class="text-muted" style="font-size:.85rem;margin-top:12px">{{ t('rb.note') }}</p>
            </div>
          </div>

          <div class="card">
            <div class="card-header"><h3>{{ t('rb.waterlines') }}</h3></div>
            <div class="card-body p-0">
              <table class="table">
                <thead><tr><th>{{ t('rb.th_node') }}</th><th>{{ t('rb.th_capacity') }}</th><th>{{ t('rb.th_used') }}</th><th>{{ t('rb.th_usedpct') }}</th></tr></thead>
                <tbody>
                  <tr v-for="n in (balance.nodes || [])" :key="n.id">
                    <td class="mono">{{ n.id }}</td>
                    <td>{{ U.humanBytes(n.capacity_gb * 1073741824) }}</td>
                    <td>{{ U.humanBytes(n.used_gb * 1073741824) }}</td>
                    <td>{{ (n.used_pct * 100).toFixed(1) }}%</td>
                  </tr>
                  <tr v-if="!(balance.nodes || []).length"><td colspan="4" class="empty">{{ t('rb.no_data') }}</td></tr>
                </tbody>
              </table>
            </div>
          </div>
        </template>

        <div v-else class="empty-state">
          <div class="ops-empty-title">{{ t('rb.no_data') }}</div>
          <div>{{ t('rb.no_data_hint') }}</div>
        </div>
        <ToastRoot/>
      </div>
    `
  };
})();
