// Chunk list — enter an inode and get its chunk references; each row drills
// into a single chunk's routed detail page (#/chunks/<id> → chunkDetail).
// Single-chunk metadata, replicas, EC layout and the commit/seal/migrate/
// delete actions all live in chunkDetail.js now.
(function () {
  'use strict';
  var Vue = window.Vue;
  var U = window.NUFS.Util;
  var A = window.NUFS.API;
  var C = window.NUFS.Components;
  var I = window.NUFS.I18n;
  var P = window.NUFS.Pages;

  P.chunks = {
    components: { Empty: C.Empty },
    props: ['id'],
    data: function () {
      return {
        inodeInput: '',
        refs: null, refsLoading: false, refsError: null,
        t: I.t
      };
    },
    mounted: function () {
      if (this.$props.id != null && this.$props.id !== '') { this.inodeInput = String(this.$props.id); }
      if (this.inodeInput) this.loadRefs();
    },
    methods: {
      loadRefs: function () {
        var self = this;
        var inode = String(this.inodeInput || '').trim();
        if (!inode) { C.toast.err(this.t('ck.enter_inode')); return; }
        this.refsLoading = true; this.refsError = null; this.refs = null;
        A.chunksByInode(inode).then(function (r) {
          self.refs = r || [];
        }).catch(function (e) {
          self.refsError = e.message; self.refs = [];
        }).finally(function () { self.refsLoading = false; });
      },
      // list row drill → routed single-chunk detail page
      goDetail: function (chunkId) { window.NUFS.Router.navigate('chunk_detail', { id: chunkId }); }
    },
    template: `
      <div>
        <div class="page-head">
          <h2 class="page-title">{{ t('ck.title') }}</h2>
          <div class="page-sub">{{ t('ck.sub') }}</div>
          <div class="actions-row">
            <input class="input" style="width:200px" v-model="inodeInput" :placeholder="t('ck.ph_inode')" @keyup.enter="loadRefs"/>
            <button class="btn" :disabled="refsLoading" @click="loadRefs">{{ refsLoading ? t('ck.loading_btn') : t('ck.load_btn') }}</button>
          </div>
        </div>

        <div v-if="refsError" class="ops-empty">
          <div class="ops-empty-title">{{ t('ck.err_load') }}</div>
          <div>{{ refsError }}</div>
        </div>

        <div v-else class="card">
          <div class="card-header"><h3>{{ t('ck.header_for', { inode: inodeInput, n: refs ? refs.length : 0 }) }}</h3></div>
          <div class="card-body p-0">
            <div v-if="refsLoading" class="loading">{{ t('common.loading') }}</div>
            <table v-else class="table">
              <thead><tr><th>{{ t('ck.th_id') }}</th><th>{{ t('ck.th_offset') }}</th><th>{{ t('ck.th_length') }}</th><th>{{ t('ck.th_version') }}</th><th></th></tr></thead>
              <tbody>
                <tr v-for="r in refs" :key="r.id" class="clickable-row" @click="goDetail(r.id)">
                  <td class="mono">{{ r.id }}</td>
                  <td class="mono">{{ r.offset }}</td>
                  <td class="mono">{{ r.length }}</td>
                  <td class="mono">{{ r.version }}</td>
                  <td style="text-align:right">
                    <button class="btn btn-sm" @click.stop="goDetail(r.id)">{{ t('ck.inspect') }}</button>
                  </td>
                </tr>
                <tr v-if="!refsLoading && refs && !refs.length"><td colspan="5" class="empty">{{ t('ck.no_chunks') }}</td></tr>
                <tr v-if="!refsLoading && !refs"><td colspan="5" class="empty">{{ t('ck.enter_inode') }}</td></tr>
              </tbody>
            </table>
          </div>
        </div>
        <ToastRoot/>
      </div>
    `
  };
})();
