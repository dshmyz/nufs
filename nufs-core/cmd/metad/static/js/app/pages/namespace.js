// Namespace file browser — tree walk over the metadata namespace (parent-inode
// based) plus namespace mutations: mkdir / create-file / make-symlink / rename
// / unlink / rmdir. Destructive ops (unlink/rmdir) go through a confirm modal.
(function () {
  'use strict';
  var Vue = window.Vue;
  var U = window.NUFS.Util;
  var A = window.NUFS.API;
  var C = window.NUFS.Components;
  var I = window.NUFS.I18n;
  var P = window.NUFS.Pages;

  var ROOT = 1;

  // Numerical FileType (0=regular, 1=dir, 2=symlink, ...).
  function isDir(t) { return t === 1; }
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
        busy: false,
        t: I.t
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
      deleteKind: function () { return isDir(this.dlgOld && this.dlgOld.type) ? this.t('ns.kind_directory') : this.t('ns.kind_file'); },
      deleteOp: function () { return isDir(this.dlgOld && this.dlgOld.type) ? this.t('ns.op_rmdir') : this.t('ns.op_unlink'); }
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
      typeLabel: function (t) {
        if (t === 0) return this.t('ns.type_file');
        if (t === 1) return this.t('ns.type_directory');
        if (t === 2) return this.t('ns.type_symlink');
        return this.t('ns.type_special');
      },
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
        if (!name) { C.toast.err(this.t('ns.toast_dirname')); return; }
        this.busy = true;
        A.mkdir(this.cur, name).then(function () { C.toast.ok(self.t('ns.toast_created_dir', { name: name })); self.closeReload(); })
          .catch(function (e) { self.busy = false; C.toast.err(e.message); });
      },
      doFile: function () {
        var self = this; var name = (this.dlgName || '').trim();
        if (!name) { C.toast.err(this.t('ns.toast_filename')); return; }
        this.busy = true;
        A.createFile(this.cur, name).then(function () { C.toast.ok(self.t('ns.toast_created_file', { name: name })); self.closeReload(); })
          .catch(function (e) { self.busy = false; C.toast.err(e.message); });
      },
      doSymlink: function () {
        var self = this; var name = (this.dlgName || '').trim(); var target = (this.dlgTarget || '').trim();
        if (!name) { C.toast.err(this.t('ns.toast_linkname')); return; }
        if (!target) { C.toast.err(this.t('ns.toast_target')); return; }
        this.busy = true;
        A.symlink(this.cur, name, target).then(function () { C.toast.ok(self.t('ns.toast_created_link', { name: name })); self.closeReload(); })
          .catch(function (e) { self.busy = false; C.toast.err(e.message); });
      },
      doRename: function () {
        var self = this; var name = (this.dlgName || '').trim();
        if (!name) { C.toast.err(this.t('ns.toast_newname')); return; }
        var old = this.dlgOld; this.busy = true;
        A.renameEntry(this.cur, old.name, this.cur, name).then(function () {
          C.toast.ok(self.t('ns.toast_renamed', { old: old.name, name: name })); self.closeReload();
        }).catch(function (e) { self.busy = false; C.toast.err(e.message); });
      },
      doDelete: function () {
        var self = this; var row = this.dlgOld; if (!row) return; this.busy = true;
        var op = isDir(row.type) ? A.rmdir(this.cur, row.name) : A.unlink(this.cur, row.name);
        op.then(function () { C.toast.ok(self.t('ns.toast_removed', { name: row.name })); self.closeReload(); })
          .catch(function (e) { self.busy = false; C.toast.err(e.message); });
      },
      closeReload: function () { this.dlg = null; this.busy = false; this.reload(); }
    },
    template: `
      <div>
        <div class="page-head">
          <h2 class="page-title">{{ t('ns.title') }}</h2>
          <div class="page-sub">{{ t('ns.sub') }}</div>
          <div class="actions-row">
            <button class="btn" @click="askMkdir" :disabled="notFound">{{ t('ns.new_folder') }}</button>
            <button class="btn" @click="askFile" :disabled="notFound">{{ t('ns.new_file') }}</button>
            <button class="btn" @click="askSymlink" :disabled="notFound">{{ t('ns.new_symlink') }}</button>
          </div>
        </div>

        <div class="crumbs" v-if="trail.length">
          <template v-for="(t, i) in trail" :key="t.id">
            <a v-if="i < trail.length - 1" href="javascript:void(0)" @click="upTo(i)">{{ t.name }}</a>
            <span v-else class="crumb-current">{{ t.name }}</span>
            <span v-if="i < trail.length - 1" class="sep">/</span>
          </template>
        </div>

        <div v-if="loading" class="loading">{{ t('ns.loading', { cur: cur }) }}</div>

        <div v-else-if="notFound" class="ops-empty">
          <div class="ops-empty-title">{{ t('ns.notfound', { cur: cur }) }}</div>
          <div v-html="t('ns.notfound_hint')"></div>
        </div>

        <div v-else-if="!entries.length" class="card"><div class="card-body"><p class="empty">{{ t('ns.empty') }}</p></div></div>

        <div v-else class="file-list">
          <template v-for="e in entries" :key="e.inode">
            <div v-if="isDir(e.type)" class="file-row dir">
              <span class="ico">{{ ico(e.type) }}</span>
              <span class="name" @click="enter(e)">{{ e.name }}/</span>
              <span class="meta">{{ t('ns.meta', { type: t('ns.type_directory'), inode: e.inode }) }}</span>
              <span class="row-actions">
                <button class="btn btn-sm" @click="askRename(e)">{{ t('common.rename') }}</button>
                <button class="btn btn-sm btn-danger" @click="askDelete(e)">{{ t('common.delete') }}</button>
              </span>
            </div>
            <div v-else :class="['file-row', 'dir-' + typeLabel(e.type)]">
              <span class="ico">{{ ico(e.type) }}</span>
              <span class="name">{{ e.name }}</span>
              <span class="meta">{{ t('ns.meta', { type: typeLabel(e.type), inode: e.inode }) }}</span>
              <span class="row-actions">
                <button class="btn btn-sm" @click="askRename(e)">{{ t('common.rename') }}</button>
                <button class="btn btn-sm btn-danger" @click="askDelete(e)">{{ t('common.delete') }}</button>
              </span>
            </div>
          </template>
        </div>

        <Modal :show="showMkdir" :title="t('ns.modal_folder')" @close="cancel">
          <input class="input" v-model="dlgName" :placeholder="t('ns.ph_folder')" @keyup.enter="doMkdir"/>
          <template #footer>
            <button class="btn" @click="cancel" :disabled="busy">{{ t('common.cancel') }}</button>
            <button class="btn btn-primary" @click="doMkdir" :disabled="busy">{{ busy ? t('ov.creating') : t('common.create') }}</button>
          </template>
        </Modal>

        <Modal :show="showFile" :title="t('ns.modal_file')" @close="cancel">
          <input class="input" v-model="dlgName" :placeholder="t('ns.ph_file')" @keyup.enter="doFile"/>
          <template #footer>
            <button class="btn" @click="cancel" :disabled="busy">{{ t('common.cancel') }}</button>
            <button class="btn btn-primary" @click="doFile" :disabled="busy">{{ busy ? t('ov.creating') : t('common.create') }}</button>
          </template>
        </Modal>

        <Modal :show="showSymlink" :title="t('ns.modal_symlink')" @close="cancel">
          <p style="font-size:.85rem">{{ t('ns.symlink_desc') }}</p>
          <input class="input" v-model="dlgName" :placeholder="t('ns.ph_link')" @keyup.enter="doSymlink"/>
          <input class="input" v-model="dlgTarget" :placeholder="t('ns.ph_target')" @keyup.enter="doSymlink" style="margin-top:8px"/>
          <template #footer>
            <button class="btn" @click="cancel" :disabled="busy">{{ t('common.cancel') }}</button>
            <button class="btn btn-primary" @click="doSymlink" :disabled="busy">{{ busy ? t('ov.creating') : t('ns.create_link') }}</button>
          </template>
        </Modal>

        <Modal :show="showRename" :title="t('ns.modal_rename')" @close="cancel">
          <p style="font-size:.85rem" v-html="t('ns.rename_desc', { name: dlgOld ? dlgOld.name : '' })"></p>
          <input class="input" v-model="dlgName" :placeholder="t('ns.ph_new')" @keyup.enter="doRename"/>
          <template #footer>
            <button class="btn" @click="cancel" :disabled="busy">{{ t('common.cancel') }}</button>
            <button class="btn btn-primary" @click="doRename" :disabled="busy">{{ busy ? t('ns.renaming') : t('common.rename') }}</button>
          </template>
        </Modal>

        <Modal :show="showDelete" :title="t('ns.modal_delete', { kind: deleteKind })" @close="cancel">
          <p style="font-size:.9rem" v-html="t('ns.delete_desc', { op: deleteOp, name: dlgOld ? dlgOld.name : '', cur: cur })"></p>
          <template #footer>
            <button class="btn" @click="cancel" :disabled="busy">{{ t('common.cancel') }}</button>
            <button class="btn btn-danger" @click="doDelete" :disabled="busy">{{ busy ? t('ns.removing') : t('common.delete') }}</button>
          </template>
        </Modal>
        <ToastRoot/>
      </div>
    `
  };
})();
