// Namespace file browser — tree walk over the metadata namespace (parent-inode
// based) plus namespace mutations: mkdir / create-file / make-symlink / rename
// / unlink / rmdir. Destructive ops (unlink/rmdir) go through a confirm modal.
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
    components: { Modal: C.Modal, Empty: C.Empty },
    data: function () {
      return {
        cur: ROOT,
        entries: [], loading: false, notFound: false,
        // breadcrumb trail of { id, name }
        trail: [{ id: ROOT, name: '/' }],
        // modal state — one active dialog at a time
        dlg: null,            // 'mkdir' | 'file' | 'symlink' | 'rename' | 'delete'
        dlgName: '', dlgTarget: '', dlgOld: null, // pending (name / target / row)
        busy: false
      };
    },
    mounted: function () { this.open(ROOT); },
    computed: {
      // which modal is open (for v-if/chained template brevity)
      showMkdir: function () { return this.dlg === 'mkdir'; },
      showFile: function () { return this.dlg === 'file'; },
      showSymlink: function () { return this.dlg === 'symlink'; },
      showRename: function () { return this.dlg === 'rename'; },
      showDelete: function () { return this.dlg === 'delete'; },
      deleteKind: function () { return this.dlgOld && isDir(this.dlgOld.type) ? 'directory' : 'file'; }
    },
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
      reload: function () { this.open(this.cur); },
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

      // ---- dialog openers ----
      askMkdir: function () { this.resetDlg('mkdir', { name: '' }); },
      askFile: function () { this.resetDlg('file', { name: '' }); },
      askSymlink: function () { this.resetDlg('symlink', { name: '', target: '' }); },
      askRename: function (e) { this.resetDlg('rename', { name: e.name, old: e }); },
      askDelete: function (e) { this.resetDlg('delete', { old: e }); },
      resetDlg: function (kind, extra) {
        this.dlg = kind;
        this.dlgName = (extra && extra.name) || '';
        this.dlgTarget = (extra && extra.target) || '';
        this.dlgOld = (extra && extra.old) || null;
        this.busy = false;
      },
      cancel: function () { if (!this.busy) { this.dlg = null; this.busy = false; } },

      // ---- submit actions ----
      doMkdir: function () {
        var self = this; var name = (this.dlgName || '').trim();
        if (!name) { C.toast.err('Enter a directory name'); return; }
        this.busy = true;
        A.mkdir(this.cur, name).then(function () { C.toast.ok('Created directory ' + name); self.closeReload(); })
          .catch(function (e) { self.busy = false; C.toast.err(e.message); });
      },
      doFile: function () {
        var self = this; var name = (this.dlgName || '').trim();
        if (!name) { C.toast.err('Enter a file name'); return; }
        this.busy = true;
        A.createFile(this.cur, name).then(function () { C.toast.ok('Created file ' + name); self.closeReload(); })
          .catch(function (e) { self.busy = false; C.toast.err(e.message); });
      },
      doSymlink: function () {
        var self = this; var name = (this.dlgName || '').trim(); var target = (this.dlgTarget || '').trim();
        if (!name) { C.toast.err('Enter a link name'); return; }
        if (!target) { C.toast.err('Enter a link target'); return; }
        this.busy = true;
        A.symlink(this.cur, name, target).then(function () { C.toast.ok('Created symlink ' + name); self.closeReload(); })
          .catch(function (e) { self.busy = false; C.toast.err(e.message); });
      },
      doRename: function () {
        var self = this; var name = (this.dlgName || '').trim();
        if (!name) { C.toast.err('Enter a new name'); return; }
        var old = this.dlgOld; this.busy = true;
        A.renameEntry(this.cur, old.name, this.cur, name).then(function () {
          C.toast.ok('Renamed ' + old.name + ' → ' + name); self.closeReload();
        }).catch(function (e) { self.busy = false; C.toast.err(e.message); });
      },
      doDelete: function () {
        var self = this; var row = this.dlgOld; if (!row) return; this.busy = true;
        var op = isDir(row.type) ? A.rmdir(this.cur, row.name) : A.unlink(this.cur, row.name);
        op.then(function () { C.toast.ok('Removed ' + row.name); self.closeReload(); })
          .catch(function (e) { self.busy = false; C.toast.err(e.message); });
      },
      closeReload: function () { this.dlg = null; this.busy = false; this.reload(); }
    },
    template: `
      <div>
        <div class="page-head">
          <h2 class="page-title">Namespace</h2>
          <div class="page-sub">Browse and manage the metadata namespace (mkdir, create, symlink, rename, delete)</div>
          <div class="actions-row">
            <button class="btn" @click="askMkdir" :disabled="notFound">New folder</button>
            <button class="btn" @click="askFile" :disabled="notFound">New file</button>
            <button class="btn" @click="askSymlink" :disabled="notFound">New symlink</button>
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

        <div v-else-if="!entries.length" class="card"><div class="card-body"><p class="empty">Empty directory. Use the buttons above to add entries.</p></div></div>

        <div v-else class="file-list">
          <template v-for="e in entries" :key="e.inode">
            <div v-if="isDir(e.type)" class="file-row dir">
              <span class="ico">{{ ico(e.type) }}</span>
              <span class="name" @click="enter(e)">{{ e.name }}/</span>
              <span class="meta">directory &middot; inode {{ e.inode }}</span>
              <span class="row-actions">
                <button class="btn btn-sm" @click="askRename(e)">Rename</button>
                <button class="btn btn-sm btn-danger" @click="askDelete(e)">Delete</button>
              </span>
            </div>
            <div v-else :class="['file-row', 'dir-' + typeLabel(e.type)]">
              <span class="ico">{{ ico(e.type) }}</span>
              <span class="name">{{ e.name }}</span>
              <span class="meta">{{ typeLabel(e.type) }} &middot; inode {{ e.inode }}</span>
              <span class="row-actions">
                <button class="btn btn-sm" @click="askRename(e)">Rename</button>
                <button class="btn btn-sm btn-danger" @click="askDelete(e)">Delete</button>
              </span>
            </div>
          </template>
        </div>

        <Modal :show="showMkdir" title="New folder" @close="cancel">
          <input class="input" v-model="dlgName" placeholder="folder name" @keyup.enter="doMkdir"/>
          <template #footer>
            <button class="btn" @click="cancel" :disabled="busy">Cancel</button>
            <button class="btn btn-primary" @click="doMkdir" :disabled="busy">{{ busy ? 'Creating…' : 'Create' }}</button>
          </template>
        </Modal>

        <Modal :show="showFile" title="New file" @close="cancel">
          <input class="input" v-model="dlgName" placeholder="file name" @keyup.enter="doFile"/>
          <template #footer>
            <button class="btn" @click="cancel" :disabled="busy">Cancel</button>
            <button class="btn btn-primary" @click="doFile" :disabled="busy">{{ busy ? 'Creating…' : 'Create' }}</button>
          </template>
        </Modal>

        <Modal :show="showSymlink" title="New symlink" @close="cancel">
          <p style="font-size:.85rem">A symlink is a named pointer to a target path; it does not create new data.</p>
          <input class="input" v-model="dlgName" placeholder="link name" @keyup.enter="doSymlink"/>
          <input class="input" v-model="dlgTarget" placeholder="target path (e.g. /dir/file)" @keyup.enter="doSymlink" style="margin-top:8px"/>
          <template #footer>
            <button class="btn" @click="cancel" :disabled="busy">Cancel</button>
            <button class="btn btn-primary" @click="doSymlink" :disabled="busy">{{ busy ? 'Creating…' : 'Create link' }}</button>
          </template>
        </Modal>

        <Modal :show="showRename" title="Rename" @close="cancel">
          <p style="font-size:.85rem">Rename <strong>{{ dlgOld ? dlgOld.name : '' }}</strong> in this directory.</p>
          <input class="input" v-model="dlgName" placeholder="new name" @keyup.enter="doRename"/>
          <template #footer>
            <button class="btn" @click="cancel" :disabled="busy">Cancel</button>
            <button class="btn btn-primary" @click="doRename" :disabled="busy">{{ busy ? 'Renaming…' : 'Rename' }}</button>
          </template>
        </Modal>

        <Modal :show="showDelete" title="Delete {{ deleteKind }}" @close="cancel">
          <p style="font-size:.9rem">Permanently {{ isDir((dlgOld||{}).type) ? 'rmdir' : 'unlink' }} <strong>{{ dlgOld ? dlgOld.name : '' }}</strong> from inode {{ cur }}?<br>
          <span style="color:var(--danger);font-size:.8rem">This cannot be undone.</span></p>
          <template #footer>
            <button class="btn" @click="cancel" :disabled="busy">Cancel</button>
            <button class="btn btn-danger" @click="doDelete" :disabled="busy">{{ busy ? 'Removing…' : 'Delete' }}</button>
          </template>
        </Modal>
        <ToastRoot/>
      </div>
    `
  };
})();
