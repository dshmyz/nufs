// Overview page — the storage-array "control room". Signature is the
// cluster-health sector ring (HeroRing) plus the fault-domain bay strip.
(function () {
  'use strict';
  var Vue = window.Vue;
  var U = window.NUFS.Util;
  var A = window.NUFS.API;
  var C = window.NUFS.Components;
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
        if (this.nodes.length === 0) return 'No nodes registered';
        if (this.offline > 0) return 'Degraded';
        return 'Cluster healthy';
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
      goBuckets: function () { window.NUFS.Router.navigate('buckets'); },
      goRepair: function () { window.NUFS.Router.navigate('repair'); }
    },
    mounted: function () {
      this.load();
      this.startStream();
    },
    beforeUnmount: function () { if (this._es) this._es.close(); },
    template: `
      <div>
        <template v-if="loading && nodes.length === 0 && buckets.length === 0">
          <div class="loading">Loading cluster…</div>
        </template>

        <template v-else-if="empty">
          <div class="empty-state">
            <div class="empty-icon"><svg width="64" height="64" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="3" width="20" height="14" rx="2" ry="2"/><line x1="8" y1="21" x2="16" y2="21"/><line x1="12" y1="17" x2="12" y2="21"/></svg></div>
            <h2>Welcome to NUFS</h2>
            <p class="empty-desc">Your cluster is empty. Create demo data to explore the console.</p>
            <button class="btn btn-primary btn-lg" :disabled="seeding" @click="runSeed">{{ seeding ? 'Creating…' : 'Create Demo Data' }}</button>
            <p v-if="seedError" class="empty-hint" style="color:#dc2626">{{ seedError }}</p>
            <p class="empty-hint">This will create 3 nodes, 2 buckets, and sample chunks + namespace entries.</p>
          </div>
        </template>

        <template v-else>
          <!-- Hero: cluster health sector ring -->
          <div class="cluster-hero">
            <HeroRing :online="online" :draining="draining" :offline="offline" :total="nodes.length"/>
            <div class="hero-body">
              <div class="hero-eyebrow"><span class="eyebrow-dot"></span> NUFS cluster &middot; {{ nodes.length }} node{{ nodes.length !== 1 ? 's' : '' }}</div>
              <div class="hero-title">{{ verdict === 'ok' ? 'Data plane nominal' : (verdict === 'degraded' ? 'Attention required' : 'No live nodes') }}</div>
              <div class="hero-desc">
                {{ nodes.length }} node{{ nodes.length !== 1 ? 's' : '' }} across {{ topo.length }} fault domain{{ topo.length !== 1 ? 's' : '' }}.
                {{ online }} online, {{ draining }} draining{{ offline ? ', ' + offline + ' offline' : '' }}.
                {{ repairs.length }} repair{{ repairs.length !== 1 ? 's' : '' }} pending.
              </div>
              <div class="hero-gauge-row">
                <div class="hero-gauge"><span class="g-k">Capacity waterline</span><span class="g-v" :class="capacityPct >= 75 ? 'heat' : 'cool'">{{ capacityPct.toFixed(1) }}%</span><div class="g-meter"><i :style="{ width: capacityPct + '%' }"></i></div></div>
                <div class="hero-gauge"><span class="g-k">Physical / logical</span><span class="g-v" :class="onDiskPct > 100 ? 'heat' : 'cool'">{{ onDiskPct.toFixed(0) }}%</span><div class="g-meter"><i :style="{ width: Math.min(onDiskPct, 100) + '%' }"></i></div></div>
                <div class="hero-gauge"><span class="g-k">Live bytes</span><span class="g-v">{{ U.humanBytes(totals.used) }}</span></div>
                <div class="hero-gauge"><span class="g-k">Physical on disk</span><span class="g-v heat">{{ U.humanBytes(totals.ondisk) }}</span></div>
              </div>
              <div class="hero-chips">
                <span class="hero-chip ok">{{ online }} online</span>
                <span v-if="draining" class="hero-chip warn">{{ draining }} draining</span>
                <span v-if="offline" class="hero-chip bad">{{ offline }} offline</span>
                <span class="hero-chip neutral">{{ buckets.length }} bucket{{ buckets.length !== 1 ? 's' : '' }}</span>
                <span class="hero-chip neutral">{{ totals.chunks }} chunks</span>
              </div>
            </div>
          </div>

          <!-- Fault-domain bay strip -->
          <FaultDomainStrip :domains="faultDomains"/>

          <!-- Balance / readiness strip -->
          <div v-if="balance" class="card">
            <div class="card-header"><h3>Rebalance posture</h3>
              <span class="badge" :class="'badge-' + balanceBadge">{{ (balance.imbalance * 100).toFixed(1) }}% spread</span>
            </div>
            <div class="card-body" style="display:flex;flex-wrap:wrap;gap:22px;align-items:center">
              <span class="text-muted" style="font-size:.85rem">Imbalance</span>
              <strong v-if="balance.nodes">{{ balance.nodes.length }} node{{ balance.nodes.length !== 1 ? 's' : '' }}</strong>
              <span class="text-muted">{{ balance.recommendation }}</span>
              <span class="badge" :class="'badge-' + (balance.total_used_pct > 0.8 ? 'warning' : 'info')" style="margin-left:auto">
                {{ (balance.total_used_pct * 100).toFixed(1) }}% used cluster-wide
              </span>
            </div>
          </div>

          <div class="stats-grid">
            <div class="stat-card stat-blue">
              <div class="stat-value">{{ nodes.length }}</div>
              <div class="stat-label">Total Nodes</div>
              <div class="stat-sub"><span class="stat-green">{{ online }} online</span><span v-if="draining"> &middot; <span class="stat-yellow">{{ draining }} draining</span></span><span v-if="offline"> &middot; <span class="stat-red">{{ offline }} offline</span></span></div>
            </div>
            <div class="stat-card stat-green"><div class="stat-value">{{ buckets.length }}</div><div class="stat-label">Buckets</div></div>
            <div class="stat-card stat-purple"><div class="stat-value">{{ totals.chunks }}</div><div class="stat-label">Total Chunks</div></div>
            <div class="stat-card stat-orange"><div class="stat-value">{{ repairs.length }}</div><div class="stat-label">Pending Repairs</div></div>
            <div class="stat-card">
              <div class="stat-value" style="color:var(--primary);font-size:1.6rem">{{ lastEvt ? lastEvt.ops_rate.toFixed(1) : '…' }}</div>
              <div class="stat-label">Ops / sec</div>
            </div>
          </div>

          <div class="card">
            <div class="card-header"><h3>Cluster Capacity</h3></div>
            <div class="card-body">
              <CapacityBar :pct="capacityPct"/>
              <div class="capacity-labels">
                <span>{{ U.humanBytes(totals.used) }} used <span class="lbl">(logical)</span></span>
                <span class="capacity-ondisk">{{ U.humanBytes(totals.ondisk) }} on disk</span>
                <span>/ {{ U.humanBytes(totals.cap) }} total ({{ capacityPct.toFixed(1) }}%)</span>
              </div>
            </div>
          </div>

          <div class="card">
            <div class="card-header"><h3>Operations Rate <span class="live-badge">LIVE</span></h3></div>
            <div class="card-body">
              <TrendChart :series="opsSeries"/>
            </div>
          </div>

          <div class="card topo-card">
            <div class="card-header">
              <h3>Node Topology</h3>
              <span class="badge" :class="nodes.length > 1 ? 'badge-info' : 'badge-secondary'">{{ nodes.length }} node{{ nodes.length !== 1 ? 's' : '' }} &middot; {{ topo.length }} fault domain{{ topo.length !== 1 ? 's' : '' }}</span>
            </div>
            <div class="card-body">
              <template v-for="(g, gi) in topo" :key="g.name">
                <div class="topo-group">
                  <div class="topo-group-name">{{ g.name }}</div>
                  <div class="topo-grid">
                    <div v-for="n in g.nodes" :key="n.id" class="tnode" :class="'tnode-' + n.stateCls">
                      <div class="tnode-head">
                        <span class="tnode-led" :class="'tnode-led-' + n.stateCls"></span>
                        <span class="tnode-id"><a href="javascript:void(0)" @click="goNode(n.id)">node {{ n.id }}</a></span>
                        <span class="tnode-state badge" :class="'badge-' + n.stateCls">{{ n.stateName }}</span>
                      </div>
                      <div v-if="n.hasCap" class="tnode-metric">
                        <RingGauge :pct="n.usagePct" :size="44" :color="n.stateCls === 'danger' ? 'danger' : (n.stateCls === 'warning' ? 'warning' : 'primary')"/>
                        <div class="tnode-ring-label">
                          <span class="tnode-pct">{{ n.usagePct.toFixed(1) }}%</span>
                          <span class="tnode-sub">{{ U.humanBytes(n.usedBytes) }} used</span>
                          <span class="tnode-sub tnode-ondisk">{{ U.humanBytes(n.onDiskBytes) }} on disk</span>
                          <span class="tnode-sub">{{ U.humanBytes(n.capBytes) }} total</span>
                        </div>
                      </div>
                      <div v-else class="tnode-metric"><span class="tnode-no-cap">capacity unknown</span></div>
                      <div class="tnode-detail">
                        <div class="tnode-row"><span class="tnode-k">chunks</span><span>{{ n.chunks }}</span></div>
                        <div v-if="n.machine" class="tnode-row"><span class="tnode-k">machine</span><span class="mono">{{ n.machine }}</span></div>
                        <div v-if="n.zone" class="tnode-row"><span class="tnode-k">zone</span><span>{{ n.zone }}</span></div>
                        <div class="tnode-row"><span class="tnode-k">addr</span><span class="mono">{{ n.addr }}</span></div>
                      </div>
                    </div>
                  </div>
                </div>
              </template>
              <p v-if="!topo.length" class="empty">No nodes registered</p>
            </div>
          </div>

          <div class="card">
            <div class="card-header"><h3>Buckets</h3><a href="javascript:void(0)" class="btn btn-sm" @click="goBuckets">View All</a></div>
            <div class="card-body p-0">
              <table class="table">
                <thead><tr><th>Name</th><th>Policy</th><th>Replicas</th></tr></thead>
                <tbody>
                  <tr v-for="b in buckets" :key="b.name"><td>{{ b.name }}</td><td>{{ b.policy ? b.policy.id : '-' }}</td><td>{{ b.policy ? b.policy.replication_factor : '-' }}</td></tr>
                  <tr v-if="!buckets.length"><td colspan="3" class="empty">No buckets</td></tr>
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
