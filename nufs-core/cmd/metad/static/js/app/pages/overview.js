// Overview page — the storage-array "control room". Signature is the
// cluster-health sector ring (HeroRing) plus the fault-domain bay strip.
(function () {
  'use strict';
  var Vue = window.Vue;
  var U = window.NUFS.Util;
  var A = window.NUFS.API;
  var C = window.NUFS.Components;
  var I = window.NUFS.I18n;
  var P = window.NUFS.Pages = window.NUFS.Pages || {};

  P.overview = {
    components: { HeroRing: C.HeroRing, FaultDomainStrip: C.FaultDomainStrip, TrendChart: C.TrendChart, StateBadge: C.StateBadge, RingGauge: C.RingGauge, CapacityBar: C.CapacityBar, Empty: C.Empty },
    data: function () {
      return {
        nodes: [], buckets: [], balance: null, repairs: [],
        loading: true, empty: false, seeding: false, seedError: '',
        // live metrics series (ops-rate) from SSE
        opsSeries: [],
        maxPoints: 60,
        lastEvt: null
      };
    },
    computed: {
      online: function () { return this.nodes.filter(function (n) { return n.state === 0; }).length; },
      draining: function () { return this.nodes.filter(function (n) { return n.state === 1 || n.state === 2; }).length; },
      offline: function () { return this.nodes.filter(function (n) { return n.state === 3 || n.state === 4; }).length; },
      totals: function () {
        var t = { cap: 0, used: 0, ondisk: 0, chunks: 0 };
        this.nodes.forEach(function (n) {
          t.cap += n.capacity_bytes || 0; t.used += n.used_bytes || 0;
          t.ondisk += n.on_disk_bytes || 0; t.chunks += n.chunk_count || 0;
        });
        return t;
      },
      capacityPct: function () { return this.totals.cap > 0 ? (this.totals.used / this.totals.cap) * 100 : 0; },
      onDiskPct: function () { return this.totals.used > 0 ? (this.totals.ondisk / this.totals.used) * 100 : 0; },
      verdict: function () {
        if (this.nodes.length === 0) return 'down';
        if (this.offline > 0) return 'degraded';
        return 'ok';
      },
      verdictLabel: function () {
        if (this.nodes.length === 0) return I.t('ov.no_nodes');
        if (this.offline > 0) return I.t('ov.hero_degraded');
        return I.t('ring.healthy');
      },
      topo: function () {
        var groups = [], byName = {};
        var self = this;
        this.nodes.forEach(function (n) {
          var domain = n.rack || (n.zone ? 'zone / ' + n.zone : 'unassigned');
          if (n.rack && n.zone) domain = n.rack + ' / ' + n.zone;
          var g = byName[domain] || (byName[domain] = { name: domain, nodes: [] });
          g.nodes.push(self._topoNode(n));
        });
        Object.keys(byName).forEach(function (k) { groups.push(byName[k]); });
        return groups;
      },
      faultDomains: function () {
        var out = [], byName = {};
        var self = this;
        this.nodes.forEach(function (n) {
          var domain = n.rack || (n.zone ? 'zone / ' + n.zone : 'unassigned');
          if (n.rack && n.zone) domain = n.rack + ' / ' + n.zone;
          if (!byName[domain]) byName[domain] = { name: domain, bays: [], total: 0, bad: 0 };
          var cls = U.stateClass(n.state) === 'success' ? 'ok' : (n.state === 1 || n.state === 2 ? 'warn' : 'down');
          if (cls === 'down') byName[domain].bad++;
          byName[domain].bays.push(cls);
          byName[domain].total++;
        });
        Object.keys(byName).forEach(function (k) { out.push(byName[k]); });
        return out;
      },
      balanceBadge: function () {
        var b = this.balance;
        if (!b) return 'secondary';
        if (b.imbalance < 0.10) return 'success';
        if (b.imbalance < 0.25) return 'info';
        if (b.imbalance < 0.50) return 'warning';
        return 'danger';
      }
    },
    methods: {
      _topoNode: function (n) {
        var usedPct = n.capacity_gb > 0 ? (n.used_gb / n.capacity_gb) * 100 : 0;
        return {
          id: n.id, addr: n.addr, rack: n.rack, zone: n.zone, machine: n.machine_id,
          capacity: n.capacity_gb || 0, used: n.used_gb || 0, ondisk: n.on_disk_gb || 0,
          usedBytes: n.used_bytes || 0, onDiskBytes: n.on_disk_bytes || 0, capBytes: n.capacity_bytes || 0,
          chunks: n.chunk_count || 0, usagePct: usedPct,
          stateCls: U.stateClass(n.state), stateName: U.stateName(n.state),
          hasCap: n.capacity_gb > 0
        };
      },
      load: function () {
        var self = this;
        this.loading = true;
        Promise.all([
          A.nodes().then(function (d) { self.nodes = d || []; self.empty = self.nodes.length === 0; return d; }),
          A.clusterBalance().then(function (d) { self.balance = d; }),
          A.buckets().then(function (d) { self.buckets = d || []; }),
          A.repairQueue().then(function (d) { self.repairs = (d && d.tasks) ? d.tasks : (Array.isArray(d) ? d : []); })
        ]).catch(function (e) { console.error('overview load', e); })
          .finally(function () { self.loading = false; });
      },
      startStream: function () {
        var self = this;
        if (this._es) { this._es.close(); }
        var es = new EventSource('/admin/metrics/stream');
        this._es = es;
        es.onmessage = function (e) {
          try {
            var d = JSON.parse(e.data);
            self.lastEvt = d;
            if (typeof d.ops_rate === 'number') {
              self.opsSeries.push(d.ops_rate);
              if (self.opsSeries.length > self.maxPoints) self.opsSeries.shift();
            }
          } catch (err) { /* ignore parse hiccups, auto-reconnect */ }
        };
        es.onerror = function () { /* EventSource auto-reconnects */ };
      },
      runSeed: function () {
        var self = this;
        this.seeding = true; this.seedError = '';
        A.seed().then(function () { window.location.reload(); })
          .catch(function (e) { self.seedError = e.message; self.seeding = false; });
      },
      goNode: function (id) { window.NUFS.Router.navigate('node_detail', { id: id }); },
      goNodes: function () { window.NUFS.Router.navigate('nodes'); },
      goQuota: function () { window.NUFS.Router.navigate('quota'); },
      goChunks: function () { window.NUFS.Router.navigate('chunks'); },
      goRepair: function () { window.NUFS.Router.navigate('repair'); },
      goRebalance: function () { window.NUFS.Router.navigate('rebalance'); }
    },
    mounted: function () {
      this.load();
      this.startStream();
    },
    beforeUnmount: function () { if (this._es) this._es.close(); },
    template: `
      <div>
        <template v-if="loading && nodes.length === 0 && buckets.length === 0">
          <div class="loading">{{ t('ov.loading') }}</div>
        </template>

        <template v-else-if="empty">
          <div class="empty-state">
            <div class="empty-icon"><svg width="64" height="64" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="3" width="20" height="14" rx="2" ry="2"/><line x1="8" y1="21" x2="16" y2="21"/><line x1="12" y1="17" x2="12" y2="21"/></svg></div>
            <h2>{{ t('ov.welcome') }}</h2>
            <p class="empty-desc">{{ t('ov.empty_desc') }}</p>
            <button class="btn btn-primary btn-lg" :disabled="seeding" @click="runSeed">{{ seeding ? t('ov.creating') : t('ov.create_demo') }}</button>
            <p v-if="seedError" class="empty-hint" style="color:var(--red)">{{ seedError }}</p>
            <p class="empty-hint">{{ t('ov.empty_hint') }}</p>
          </div>
        </template>

        <template v-else>
          <!-- Hero: cluster health sector ring -->
          <div class="cluster-hero">
            <HeroRing :online="online" :draining="draining" :offline="offline" :total="nodes.length"/>
            <div class="hero-body">
              <div class="hero-eyebrow"><span class="eyebrow-dot"></span> {{ t('ov.hero_eyebrow', { n: nodes.length }) }}</div>
              <div class="hero-title">{{ verdict === 'ok' ? t('ov.hero_ok') : (verdict === 'degraded' ? t('ov.hero_degraded') : t('ov.hero_down')) }}</div>
              <div class="hero-desc">
                {{ t('ov.hero_desc', { nodes: nodes.length, doms: topo.length, online: online, draining: draining, offline: offline ? ', ' + offline + ' offline' : '', repairs: repairs.length }) }}
              </div>
              <div class="hero-gauge-row">
                <div class="hero-gauge"><span class="g-k">{{ t('ov.g_capacity') }}</span><span class="g-v" :class="capacityPct >= 75 ? 'heat' : 'cool'">{{ capacityPct.toFixed(1) }}%</span><div class="g-meter"><i :style="{ width: capacityPct + '%' }"></i></div></div>
                <div class="hero-gauge"><span class="g-k">{{ t('ov.g_phys_logic') }}</span><span class="g-v" :class="onDiskPct > 100 ? 'heat' : 'cool'">{{ onDiskPct.toFixed(0) }}%</span><div class="g-meter"><i :style="{ width: Math.min(onDiskPct, 100) + '%' }"></i></div></div>
                <div class="hero-gauge"><span class="g-k">{{ t('ov.g_live_bytes') }}</span><span class="g-v">{{ U.humanBytes(totals.used) }}</span></div>
                <div class="hero-gauge"><span class="g-k">{{ t('ov.g_phys_disk') }}</span><span class="g-v heat">{{ U.humanBytes(totals.ondisk) }}</span></div>
              </div>
              <div class="hero-chips">
                <span class="hero-chip ok">{{ t('ov.chip_online', { n: online }) }}</span>
                <span v-if="draining" class="hero-chip warn">{{ t('ov.chip_draining', { n: draining }) }}</span>
                <span v-if="offline" class="hero-chip bad">{{ t('ov.chip_offline', { n: offline }) }}</span>
                <span class="hero-chip neutral">{{ t('ov.chip_buckets', { n: buckets.length }) }}</span>
                <span class="hero-chip neutral">{{ t('ov.chip_chunks', { n: totals.chunks }) }}</span>
              </div>
            </div>
          </div>

          <!-- Fault-domain bay strip -->
          <FaultDomainStrip :domains="faultDomains"/>

          <!-- Balance / readiness strip -->
          <div v-if="balance" class="card clickable-card" @click="goRebalance">
            <div class="card-header"><h3>{{ t('ov.rebalance_posture') }}</h3>
              <span class="badge" :class="'badge-' + balanceBadge">{{ t('ov.spread', { n: (balance.imbalance * 100).toFixed(1) }) }}</span>
            </div>
            <div class="card-body" style="display:flex;flex-wrap:wrap;gap:22px;align-items:center">
              <span class="text-muted" style="font-size:.85rem">{{ t('ov.imbalance') }}</span>
              <strong v-if="balance.nodes">{{ balance.nodes.length }} {{ t('ov.stat_nodes') }}</strong>
              <span class="text-muted">{{ balance.recommendation }}</span>
              <span class="badge" :class="'badge-' + (balance.total_used_pct > 0.8 ? 'warning' : 'info')" style="margin-left:auto">
                {{ t('ov.used_cluster_wide', { n: (balance.total_used_pct * 100).toFixed(1) }) }}
              </span>
            </div>
          </div>

          <div class="stats-grid">
            <div class="stat-card stat-blue" @click="goNodes">
              <div class="stat-value">{{ nodes.length }}</div>
              <div class="stat-label">{{ t('ov.stat_nodes') }}</div>
              <div class="stat-sub"><span class="stat-green">{{ online }} {{ t('ov.chip_online_lbl') }}</span><span v-if="draining"> &middot; <span class="stat-yellow">{{ draining }} {{ t('ov.chip_draining_lbl') }}</span></span><span v-if="offline"> &middot; <span class="stat-red">{{ offline }} {{ t('ov.chip_offline_lbl') }}</span></span></div>
            </div>
            <div class="stat-card stat-green" @click="goQuota"><div class="stat-value">{{ buckets.length }}</div><div class="stat-label">{{ t('ov.stat_buckets') }}</div></div>
            <div class="stat-card stat-purple" @click="goChunks"><div class="stat-value">{{ totals.chunks }}</div><div class="stat-label">{{ t('ov.stat_chunks') }}</div></div>
            <div class="stat-card stat-orange" @click="goRepair"><div class="stat-value">{{ repairs.length }}</div><div class="stat-label">{{ t('ov.stat_repairs') }}</div></div>
            <div class="stat-card" style="cursor:default">
              <div class="stat-value" style="color:var(--primary);font-size:1.6rem">{{ lastEvt ? lastEvt.ops_rate.toFixed(1) : '…' }}</div>
              <div class="stat-label">{{ t('ov.stat_ops_sec') }}</div>
            </div>
          </div>

          <div class="card">
            <div class="card-header"><h3>{{ t('ov.capacity') }}</h3></div>
            <div class="card-body">
              <CapacityBar :pct="capacityPct"/>
              <div class="capacity-labels">
                <span>{{ t('ov.capacity_used', { used: U.humanBytes(totals.used) }) }}</span>
                <span class="capacity-ondisk">{{ t('ov.ondisk', { n: U.humanBytes(totals.ondisk) }) }}</span>
                <span>{{ t('ov.capacity_total', { total: U.humanBytes(totals.cap), pct: capacityPct.toFixed(1) }) }}</span>
              </div>
            </div>
          </div>

          <div class="card">
            <div class="card-header"><h3>{{ t('ov.ops_rate') }} <span class="live-badge">{{ t('ov.live') }}</span></h3></div>
            <div class="card-body">
              <TrendChart :series="opsSeries"/>
            </div>
          </div>

          <div class="card topo-card">
            <div class="card-header">
              <h3>{{ t('ov.topo') }}</h3>
              <span class="badge" :class="nodes.length > 1 ? 'badge-info' : 'badge-secondary'">{{ t('ov.topo_badge', { n: nodes.length, d: topo.length }) }}</span>
            </div>
            <div class="card-body">
              <template v-for="(g, gi) in topo" :key="g.name">
                <div class="topo-group">
                  <div class="topo-group-name">{{ g.name }}</div>
                  <div class="topo-grid">
                    <div v-for="n in g.nodes" :key="n.id" class="tnode" :class="'tnode-' + n.stateCls" @click="goNode(n.id)">
                      <div class="tnode-head">
                        <span class="tnode-led" :class="'tnode-led-' + n.stateCls"></span>
                        <span class="tnode-id"><a href="javascript:void(0)" @click="goNode(n.id)">{{ t('ov.node_n', { n: n.id }) }}</a></span>
                        <span class="tnode-state badge" :class="'badge-' + n.stateCls">{{ U.stateLabel(n.state) }}</span>
                      </div>
                      <div v-if="n.hasCap" class="tnode-metric">
                        <RingGauge :pct="n.usagePct" :size="44" :color="n.stateCls === 'danger' ? 'danger' : (n.stateCls === 'warning' ? 'warning' : 'primary')"/>
                        <div class="tnode-ring-label">
                          <span class="tnode-pct">{{ n.usagePct.toFixed(1) }}%</span>
                          <span class="tnode-sub">{{ t('ov.used', { n: U.humanBytes(n.usedBytes) }) }}</span>
                          <span class="tnode-sub tnode-ondisk">{{ t('ov.ondisk_short', { n: U.humanBytes(n.onDiskBytes) }) }}</span>
                          <span class="tnode-sub">{{ t('ov.total_short', { n: U.humanBytes(n.capBytes) }) }}</span>
                        </div>
                      </div>
                      <div v-else class="tnode-metric"><span class="tnode-no-cap">{{ t('ov.cap_unknown') }}</span></div>
                      <div class="tnode-detail">
                        <div class="tnode-row"><span class="tnode-k">{{ t('ov.row_chunks') }}</span><span>{{ n.chunks }}</span></div>
                        <div v-if="n.machine" class="tnode-row"><span class="tnode-k">{{ t('ov.row_machine') }}</span><span class="mono">{{ n.machine }}</span></div>
                        <div v-if="n.zone" class="tnode-row"><span class="tnode-k">{{ t('ov.row_zone') }}</span><span>{{ n.zone }}</span></div>
                        <div class="tnode-row"><span class="tnode-k">{{ t('ov.row_addr') }}</span><span class="mono">{{ n.addr }}</span></div>
                      </div>
                    </div>
                  </div>
                </div>
              </template>
              <p v-if="!topo.length" class="empty">{{ t('ov.no_nodes') }}</p>
            </div>
          </div>

          <div class="card">
            <div class="card-header"><h3>{{ t('ov.buckets') }}</h3><a href="javascript:void(0)" class="btn btn-sm" @click="goQuota">{{ t('ov.view_all') }}</a></div>
            <div class="card-body p-0">
              <table class="table">
                <thead><tr><th>{{ t('ov.th_name') }}</th><th>{{ t('ov.th_policy') }}</th><th>{{ t('ov.th_replicas') }}</th></tr></thead>
                <tbody>
                  <tr v-for="b in buckets" :key="b.name"><td>{{ b.name }}</td><td>{{ b.policy ? b.policy.id : '-' }}</td><td>{{ b.policy ? b.policy.replication_factor : '-' }}</td></tr>
                  <tr v-if="!buckets.length"><td colspan="3" class="empty">{{ t('ov.th_nobuckets') }}</td></tr>
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
