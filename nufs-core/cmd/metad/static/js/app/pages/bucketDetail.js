// Bucket detail — drill from a bucket row into one bucket's quota, live usage,
// policy, and a namespace listing of its root directory.
(function () {
  'use strict';
  var Vue = window.Vue;
  var U = window.NUFS.Util;
  var A = window.NUFS.API;
  var C = window.NUFS.Components;
  var I = window.NUFS.I18n;
  var P = window.NUFS.Pages;

  function fileTypeLabel(t, self) {
    // FileType: 0=regular, 1=dir, 2=symlink
    if (t === 1) return self.t('bd.type_dir');
    if (t === 2) return self.t('bd.type_link');
    return self.t('bd.type_file');
  }
  function fileIcon(t) {
    if (t === 1) return '▸';
    if (t === 2) return '→';
    return '·';
  }

  P.bucketDetail = {
    props: ['id'],
    components: { CapacityBar: C.CapacityBar, StateBadge: C.StateBadge, Empty: C.Empty },
    data: function () {
      return { bucket: null, quota: null, entries: null, loading: true, notFound: false, t: I.t };
    },
    watch: { id: function () { this.load(); } },
    mounted: function () { this.load(); },
    computed: {
      usage: function () { return (this.quota && this.quota.usage) || { used_bytes: 0, objects: 0 }; },
      ratios: function () { return (this.quota && this.quota.ratios) || { bytes: 0, objects: 0 }; },
      hasQuota: function () {
        var q = this.quota && this.quota.quota;
        return q && (q.max_bytes != null || q.max_objects != null);
      },
      bytePct: function () { return Math.round((this.ratios.bytes || 0) * 100); },
      capacityText: function () {
        if (!this.hasQuota) return this.t('bd.unlimited');
        var q = this.quota.quota;
        var parts = [];
        if (q.max_bytes != null) parts.push(U.humanBytes(q.max_bytes));
        if (q.max_objects != null) parts.push(q.max_objects + ' obj');
        return parts.join(' / ');
      }
    },
    methods: {
      back: function () { window.NUFS.Router.navigate('quota'); },
      openNamespace: function () {
        var root = this.bucket ? this.bucket.root_inode : 1;
        // Route to namespace and deep-link its root to this bucket's inode.
        // The namespace page's mounted() reads the ?root= param and opens there.
        window.location.hash = '#/namespace?root=' + encodeURIComponent(root);
      },
      fmt: function (ts) { return U.fmtTime(ts, false); },
      typeLabel: function (t) { return fileTypeLabel(t, this); },
      fileIco: fileIcon,
      load: function () {
        var self = this;
        var name = this.id;
        this.loading = true; this.notFound = false;
        var quotaP = A.bucketQuota(name).catch(function () { return null; });
        Promise.all([A.buckets(), quotaP]).then(function (res) {
          var list = res[0] || [];
          var b = null;
          for (var i = 0; i < list.length; i++) {
            if (list[i].name === name) { b = list[i]; break; }
          }
          if (!b) { self.notFound = true; return; }
          self.bucket = b;
          self.quota = res[1];
          // List the bucket's root directory for a lightweight file entry.
          return A.readDir(b.root_inode).then(function (entries) {
            self.entries = entries || [];
          }).catch(function () { self.entries = []; });
        }).catch(function (e) { console.error(e); self.notFound = true; })
          .finally(function () { self.loading = false; });
      }
    },
    template: `
      <div>
        <div v-if="loading" class="loading">{{ t('bd.loading', { name: id }) }}</div>

        <div v-else-if="notFound" class="ops-empty">
          <div class="ops-empty-title">{{ t('bd.notfound', { name: id }) }}</div>
          <div v-html="t('bd.notfound_hint')"></div>
        </div>

        <template v-else-if="bucket">
          <div class="page-head">
            <h2 class="page-title">{{ t('page.bucket_detail') }} — <span class="mono">{{ bucket.name }}</span></h2>
            <div class="page-sub"><a href="javascript:void(0)" @click="back">{{ t('bd.back') }}</a></div>
          </div>

          <div class="card">
            <div class="card-header"><h3>{{ t('bd.overview') }}</h3></div>
            <div class="card-body">
              <div class="kv-grid">
                <div class="kv"><div class="k">{{ t('bd.k_root_inode') }}</div><div class="v mono">{{ bucket.root_inode }}</div></div>
                <div class="kv"><div class="k">{{ t('bd.k_created') }}</div><div class="v">{{ fmt(bucket.creation_date) }}</div></div>
                <div class="kv"><div class="k">{{ t('bd.k_policy') }}</div><div class="v mono">{{ bucket.policy && (bucket.policy.id || bucket.policy.name) || '—' }}</div></div>
                <div class="kv"><div class="k">{{ t('bd.k_replication') }}</div><div class="v mono">{{ bucket.policy ? bucket.policy.replication_factor : '—' }}</div></div>
                <div class="kv"><div class="k">{{ t('bd.k_storage') }}</div><div class="v">
                  <span v-if="bucket.policy && bucket.policy.ec_config" class="badge badge-info">{{ t('bd.ec_yes', { d: bucket.policy.ec_config.data_shards, p: bucket.policy.ec_config.parity_shards }) }}</span>
                  <span v-else class="badge badge-secondary">{{ t('bd.ec_no', { rf: bucket.policy ? bucket.policy.replication_factor : '—' }) }}</span>
                </div></div>
              </div>
            </div>
          </div>

          <div class="card">
            <div class="card-header"><h3>{{ t('bd.quota') }}</h3></div>
            <div class="card-body">
              <div class="kv-grid">
                <div class="kv"><div class="k">{{ t('bd.quota_max') }}</div><div class="v mono">{{ capacityText }}</div></div>
                <div class="kv"><div class="k">{{ t('bd.used_bytes') }}</div><div class="v mono">{{ U.humanBytes(usage.used_bytes) }}</div></div>
                <div class="kv"><div class="k">{{ t('bd.objects') }}</div><div class="v mono">{{ usage.objects }}</div></div>
                <div class="kv"><div class="k">{{ t('bd.ratio') }}</div><div class="v mono">{{ bytePct }}%</div></div>
              </div>
              <div v-if="hasQuota" style="margin-top:12px">
                <CapacityBar :pct="bytePct"/>
              </div>
              <div v-else class="text-muted" style="font-size:.85rem;margin-top:12px">{{ t('bd.unlimited') }}</div>
            </div>
          </div>

          <div class="card">
            <div class="card-header"><h3>{{ t('bd.files', { inode: bucket.root_inode }) }} <a href="javascript:void(0)" class="btn btn-sm" @click="openNamespace">{{ t('bd.open_namespace') }}</a></h3></div>
            <div class="card-body p-0">
              <div class="page-sub" style="padding:8px 16px">{{ t('bd.files_hint') }}</div>
              <table class="table">
                <thead><tr><th>{{ t('bd.th_name') }}</th><th>{{ t('bd.th_type') }}</th><th>{{ t('bd.th_inode') }}</th></tr></thead>
                <tbody>
                  <tr v-for="e in entries" :key="e.inode">
                    <td><span class="muted">{{ fileIco(e.type) }}</span> {{ e.name }}</td>
                    <td>{{ typeLabel(e.type) }}</td>
                    <td class="mono">{{ e.inode }}</td>
                  </tr>
                  <tr v-if="!entries || !entries.length"><td colspan="3" class="empty">{{ t('bd.no_files') }}</td></tr>
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
