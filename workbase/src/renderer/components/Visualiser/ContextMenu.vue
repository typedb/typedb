<template>
    <div v-show="contextMenu.show" ref="contextMenu" id="context-menu" class="z-depth-2">
        <li @click="(enableDelete) ? deleteNode() : false" class="context-action delete-nodes" :class="{'disabled':!enableDelete}">Delete</li>
        <li @click="(enableExplain) ? explainNode() : false" class="context-action explain-node" :class="{'disabled':!enableExplain}">Explain</li>
        <li @click="(enableShortestPath) ? computeShortestPath() : false" class="context-action compute-shortest-path" :class="{'disabled':!enableShortestPath}">Shortest Path</li>
    </div>
</template>
<script>
  import { RUN_CURRENT_QUERY, EXPLAIN_CONCEPT, DELETE_SELECTED_NODES } from '@/components/shared/StoresActions';
  import { mapGetters } from 'vuex';


  export default {
    name: 'ContextMenu',
    computed: {
      ...mapGetters(['selectedNodes', 'currentKeyspace', 'contextMenu', 'visFacade']),
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
        this.$store.commit('contextMenu', { show: false, x: null, y: null });
        this.$store.dispatch(DELETE_SELECTED_NODES).catch((err) => { this.$notifyError(err, 'Delete nodes'); });
      },
      explainNode() {
        this.$store.commit('contextMenu', { show: false, x: null, y: null });
        this.$store.dispatch(EXPLAIN_CONCEPT).catch((err) => { this.$notifyError(err, 'Explain Concept'); });
      },
      computeShortestPath() {
        this.$store.commit('contextMenu', { show: false, x: null, y: null });
        this.$store.commit('currentQuery', `compute path from "${this.selectedNodes[0].id}", to "${this.selectedNodes[1].id}";`);
        this.$store.dispatch(RUN_CURRENT_QUERY).catch((err) => { this.$notifyError(err, 'Run Query'); });
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


