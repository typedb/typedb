<template>
  <div v-show="showContextMenu" ref="menu" id="context-menu" class="z-depth-2">
      <div @click="openNewType" class="context-action">
        New Type
      </div>

      <div @click="askConfirmation" class="context-action" :class="{'disabled':!selectedNode}">
        Delete Type
      </div>
  </div>
</template>
<script>
import { DELETE_TYPE } from '../../shared/StoresActions';

export default {
  name: 'ContextMenu',
  props: ['localStore'],
  data() {
    return {
      showContextMenu: false,
    };
  },

  computed: {
    selectedNode() {
      return this.localStore.getSelectedNode();
    },
    readyToRegisterEvents() {
      return this.localStore.isInit;
    },
  },
  watch: {
    readyToRegisterEvents() {
      this.localStore.registerCanvasEventHandler('oncontext', (mouseEvent) => {
        this.repositionMenu(mouseEvent);
        this.showContextMenu = true;
      });
      this.localStore.registerCanvasEventHandler('click', () => { this.showContextMenu = false; });
      this.localStore.registerCanvasEventHandler('selectNode', () => { this.showContextMenu = false; });
      this.localStore.registerCanvasEventHandler('deselectNode', () => { this.showContextMenu = false; });
      this.localStore.registerCanvasEventHandler('dragStart', () => { this.showContextMenu = false; });
    },
  },
  methods: {
    deleteSelectedNode() {
      if (!this.selectedNode) return;
      const { id } = this.selectedNode;
      const label = this.selectedNode.label;
      this.localStore.dispatch(DELETE_TYPE, { id, label })
        .then(() => { this.localStore.setSelectedNode(null); })
        .catch((err) => { this.$notifyError(err); });
    },
    askConfirmation() {
      if (!this.selectedNode) return;
      this.showContextMenu = false;
      this.$notifyConfirmDelete(`Confirm deletion of [${this.selectedNode.label}]`, this.deleteSelectedNode);
    },
    openNewType() {
      this.$emit('open-new-type-panel');
      this.showContextMenu = false;
    },
    repositionMenu(mouseEvent) {
      const contextMenu = this.$refs.menu;
      contextMenu.style.left = `${mouseEvent.pointer.DOM.x}px`;
      contextMenu.style.top = `${mouseEvent.pointer.DOM.y}px`;
    },
  },

};
</script>
<style>
#context-menu{
  position: absolute;
  border-radius: 4px;
  background-color: #282828;
  padding: 8px 0px;
  z-index: 10;
  min-width: 100px;
}

.disabled{
  opacity: 0.5;
  cursor: default;
}

.context-action{
  padding:8px 10px;
  cursor: pointer;
  opacity: 0.8;
}

.context-action:not(.disabled):hover{
  opacity: 1;
  background-color: #404040;
}
</style>


