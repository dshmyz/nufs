// Namespace file browser — read-only tree walk over the metadata namespace
// (parent-inode based) plus mkdir. Uses the card-list .file-row style.
(function () {
  'use strict';
  var Vue = window.Vue;
  var U = window.NUFS.Util;
  var A = window.NUFS.API;
  var C = window.NUFS.Components;
  var P = window.NUFS.Pages;

  var ROOT = 1;

  // Numerical FileType (0=regular, 1=dir, 2=symlink, ...).
  function isDir(t) { return t === 1; }
  function typeLabel(t) {
    if (t === 0) return 'file';
    if (t === 1) return 'directory';
    if (t === 2) return 'symlink';
    return 'special';
  }
  function ico(t) {
    if (t === 1) return '▸';
    if (t === 2) return '→';
    return '·';
  }

  P.namespace = {
    components: { Modal: C.Modal },
    data: function () {
      return {
        cur: ROOT,
        entries: [], loading: false, notFound: false,
        // breadcrumb trail of { id, name }
        trail: [{ id: ROOT, name: '/' }],
        mkdirOpen: false, mkdirName: '', mkdirBusy: false
      };
    },
    mounted: function () { this.open(ROOT); },
    methods: {
      open: function (id) {
        var self = this;
        this.cur = id;
        this.loading = true; this.notFound = false;
        A.readDir(id).then(function (entries) {
          // Backend returns JSON `null` for an empty directory and a 500
          // error for a genuinely missing inode. So null/empty = empty dir.
          self.entries = entries || [];
        }).catch(function (e) { console.error(e); self.notFound = true; })
          .finally(function () { self.loading = false; });
      },
      enter: function (e) {
        if (!isDir(e.type)) return;
        this.trail.push({ id: e.inode, name: e.name });
        this.open(e.inode);
      },
      upTo: function (idx) {
        this.trail = this.trail.slice(0, idx + 1);
        this.open(this.trail[idx].id);
      },
      isDir: isDir,
      typeLabel: typeLabel,
      ico: ico,
      askMkdir: function () { this.mkdirOpen = true; this.mkdirName = ''; },
      doMkdir: function () {
        var self = this;
        var name = (this.mkdirName || '').trim();
        if (!name) { C.toast.err('Enter a directory name'); return; }
        this.mkdirBusy = true;
        A.mkdir(this.cur, name).then(function () {
          C.toast.ok('Created ' + name);
          self.mkdirOpen = false; self.mkdirBusy = false;
          self.open(self.cur);
        }).catch(function (e) { self.mkdirBusy = false; C.toast.err(e.message); });
      }
    },
    template: `
      <div>
        <div class="page-head">
          <h2 class="page-title">Namespace</h2>
          <div class="page-sub">Read-only tree walk over the metadata namespace</div>
          <div class="actions-row">
            <button class="btn" @click="askMkdir" :disabled="notFound">New folder</button>
          </div>
        </div>

        <div class="crumbs" v-if="trail.length">
          <template v-for="(t, i) in trail" :key="t.id">
            <a v-if="i < trail.length - 1" href="javascript:void(0)" @click="upTo(i)">{{ t.name }}</a>
            <span v-else class="crumb-current">{{ t.name }}</span>
            <span v-if="i < trail.length - 1" class="sep">/</span>
          </template>
        </div>

        <div v-if="loading" class="loading">Reading inode {{ cur }}…</div>

        <div v-else-if="notFound" class="ops-empty">
          <div class="ops-empty-title">Inode {{ cur }} not found</div>
          <div>This directory may have been removed. <a href="javascript:void(0)" @click="upTo(0)">Back to root</a></div>
        </div>

        <div v-else-if="!entries.length" class="card"><div class="card-body"><p class="empty">Empty directory</p></div></div>

        <div v-else class="file-list">
          <template v-for="e in entries" :key="e.inode">
            <div v-if="isDir(e.type)" class="file-row dir" @click="enter(e)">
              <span class="ico">{{ ico(e.type) }}</span>
              <span class="name">{{ e.name }}/</span>
              <span class="meta">directory &middot; inode {{ e.inode }}</span>
              <span class="meta" style="color:var(--primary)">open →</span>
            </div>
            <div v-else :class="['file-row', 'dir-' + typeLabel(e.type)]" style="cursor:default">
              <span class="ico">{{ ico(e.type) }}</span>
              <span class="name">{{ e.name }}</span>
              <span class="meta">{{ typeLabel(e.type) }} &middot; inode {{ e.inode }}</span>
            </div>
          </template>
        </div>

        <Modal :show="mkdirOpen" title="New folder" @close="mkdirOpen = false">
          <input class="input" v-model="mkdirName" placeholder="folder name" @keyup.enter="doMkdir"/>
          <template #footer>
            <button class="btn" @click="mkdirOpen = false" :disabled="mkdirBusy">Cancel</button>
            <button class="btn btn-primary" @click="doMkdir" :disabled="mkdirBusy">{{ mkdirBusy ? 'Creating…' : 'Create' }}</button>
          </template>
        </Modal>
        <ToastRoot/>
      </div>
    `
  };
})();
