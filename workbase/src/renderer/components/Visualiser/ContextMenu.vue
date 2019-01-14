<template>
    <div v-show="contextMenu.show" ref="contextMenu" id="context-menu" class="z-depth-2">
        <li @click="(enableDelete) ? deleteNode() : false" class="context-action delete-nodes" :class="{'disabled':!enableDelete}">Hide</li>
        <li @click="(enableExplain) ? explainNode() : false" class="context-action explain-node" :class="{'disabled':!enableExplain}">Explain</li>
        <li @click="(enableShortestPath) ? computeShortestPath() : false" class="context-action compute-shortest-path" :class="{'disabled':!enableShortestPath}">Shortest Path</li>
    </div>
</template>
<script>
  import { RUN_CURRENT_QUERY, EXPLAIN_CONCEPT, DELETE_SELECTED_NODES } from '@/components/shared/StoresActions';
  import { createNamespacedHelpers } from 'vuex';


  export default {
    name: 'ContextMenu',
    props: ['tabId'],
    beforeCreate() {
      const { mapGetters, mapMutations, mapActions } = createNamespacedHelpers(`tab-${this.$options.propsData.tabId}`);

      // computed
      this.$options.computed = {
        ...(this.$options.computed || {}),
        ...mapGetters(['currentKeyspace', 'contextMenu', 'selectedNodes']),
      };

      // methods
      this.$options.methods = {
        ...(this.$options.methods || {}),
        ...mapMutations(['setCurrentQuery', 'setContextMenu']),
        ...mapActions([RUN_CURRENT_QUERY, DELETE_SELECTED_NODES, EXPLAIN_CONCEPT]),
      };
    },
    computed: {
      enableDelete() {
        return (this.selectedNodes);
      },
      enableExplain() {
        return (this.selectedNodes && this.selectedNodes[0].isInferred);
      },
      enableShortestPath() {
        return (this.selectedNodes && this.selectedNodes.length === 2);
      },
    },
    watch: {
      contextMenu(contextMenu) {
        this.$refs.contextMenu.style.left = `${contextMenu.x}px`;
        this.$refs.contextMenu.style.top = `${contextMenu.y}px`;
      },
    },
    methods: {
      deleteNode() {
        this.setContextMenu({ show: false, x: null, y: null });
        this[DELETE_SELECTED_NODES]().catch((err) => { this.$notifyError(err, 'Delete nodes'); });
      },
      explainNode() {
        this.setContextMenu({ show: false, x: null, y: null });
        this[EXPLAIN_CONCEPT]().catch((err) => { this.$notifyError(err, 'Explain Concept'); });
      },
      computeShortestPath() {
        this.setContextMenu({ show: false, x: null, y: null });
        this.setCurrentQuery(`compute path from "${this.selectedNodes[0].id}", to "${this.selectedNodes[1].id}";`);
        this[RUN_CURRENT_QUERY]().catch((err) => { this.$notifyError(err, 'Run Query'); });
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


