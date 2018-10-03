<template>
    <div v-show="showContextMenu" id="context-menu" class="z-depth-2">
        <li @click="(enableDelete) ? deleteNode() : false" class="context-action" :class="{'disabled':!enableDelete}">{{deleteNodeText}}</li>
        <li @click="(enableExplain) ? explainNode() : false" class="context-action" :class="{'disabled':!enableExplain}">Explain</li>
        <li @click="(enableShortestPath) ? computeShortestPath() : false" class="context-action" :class="{'disabled':!enableShortestPath}">Shortest Path</li>
    </div>
</template>
<script>
  import { RUN_CURRENT_QUERY, EXPLAIN_CONCEPT } from '@/components/shared/StoresActions';

  export default {
    name: 'ContextMenu',
    data() {
      return {
        showContextMenu: false,
        enableShortestPath: false,
        enableExplain: false,
        enableDelete: false,
        deleteNodeText: 'Delete Node',
      };
    },
    created() {
      // this.$store.registerCanvasEventHandler('oncontext', (params) => {
      //   // Do not show context menu when keyspace is not selected or canvasData is empty
      //   if (!this.currentKeyspace || (!this.localStore.canvasData.entities && !this.localStore.canvasData.attributes && !this.localStore.canvasData.relationships)) return;

      //   // check which menu items to enable
      //   this.enableDelete = this.verifyEnableDelete();
      //   this.enableExplain = this.verifyEnableExplain();
      //   this.enableShortestPath = this.verifyEnableShortestPath();

      //   this.repositionMenu(params);
      //   this.showContextMenu = true;
      // });
      // this.localStore.registerCanvasEventHandler('click', () => { this.showContextMenu = false; });
      // this.localStore.registerCanvasEventHandler('selectNode', () => { this.showContextMenu = false; });
      // this.localStore.registerCanvasEventHandler('deselectNode', () => { this.showContextMenu = false; });
      // this.localStore.registerCanvasEventHandler('dragStart', () => { this.showContextMenu = false; });
      // this.localStore.registerCanvasEventHandler('zoom', () => { this.showContextMenu = false; });
    },
    computed: {
      selectedNodes() {
        return this.localStore.getSelectedNodes();
      },
      currentKeyspace() {
        return this.localStore.currentKeyspace;
      },
    },
    methods: {
      deleteNode() {
        this.selectedNodes.forEach((node) => {
          this.localStore.visFacade.container.visualiser.deleteNode(node);
        });
        this.localStore.setSelectedNodes(null);
        this.showContextMenu = false;
      },
      repositionMenu(mouseEvent) {
        const contextMenu = document.getElementById('context-menu');
        contextMenu.style.left = `${mouseEvent.pointer.DOM.x}px`;
        contextMenu.style.top = `${mouseEvent.pointer.DOM.y}px`;
      },
      explainNode() {
        this.localStore.dispatch(EXPLAIN_CONCEPT).catch((err) => { this.$notifyError(err, 'Explain Concept'); });
        this.showContextMenu = false;
      },
      verifyEnableExplain() {
        return (this.selectedNodes && this.selectedNodes[0].isInferred);
      },
      verifyEnableDelete() {
        if (!this.selectedNodes) {
          this.deleteNodeText = 'Delete Node';
          return false;
        } else if (this.selectedNodes.length > 1) {
          this.deleteNodeText = 'Delete Nodes';
          return true;
        }
        this.deleteNodeText = 'Delete Node';
        return true;
      },
      verifyEnableShortestPath() {
        return (this.selectedNodes && this.selectedNodes.length === 2);
      },
      computeShortestPath() {
        this.localStore.currentQuery = `compute path from "${this.selectedNodes[0].id}", to "${this.selectedNodes[1].id}";`;
        this.localStore.dispatch(RUN_CURRENT_QUERY).catch((err) => { this.$notifyError(err, 'Run Query'); });
        this.showContextMenu = false;
      },
    },
  };
</script>
<style>
    #context-menu{
        position: absolute;
        background-color: #282828;
        padding: 8px 0px;
        z-index: 10;
        min-width: 100px;
        border: var(--container-darkest-border);
    }

    .context-action.disabled{
        opacity: 0.2;
        cursor: default;
    }

    .context-action{
        padding:8px 10px;
        cursor: pointer;
        opacity: 0.8;
        list-style-type: none;
    }

    .context-action:not(.disabled):hover{
        opacity: 1;
        background-color: #404040;
    }
</style>


