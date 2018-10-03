<template>
    <div v-show="contextMenu.show" ref="contextMenu" id="context-menu" class="z-depth-2">
        <li @click="(verifyEnableDelete) ? deleteNode() : false" class="context-action" :class="{'disabled':!verifyEnableDelete}">Delete</li>
        <li @click="(verifyEnableExplain) ? explainNode() : false" class="context-action" :class="{'disabled':!verifyEnableExplain}">Explain</li>
        <li @click="(verifyEnableShortestPath) ? computeShortestPath() : false" class="context-action" :class="{'disabled':!verifyEnableShortestPath}">Shortest Path</li>
    </div>
</template>
<script>
  import { RUN_CURRENT_QUERY, EXPLAIN_CONCEPT } from '@/components/shared/StoresActions';
  import { mapGetters } from 'vuex';


  export default {
    name: 'ContextMenu',
    computed: {
      ...mapGetters(['selectedNodes', 'currentKeyspace', 'contextMenu', 'contextMenuOptions', 'visFacade']),
      verifyEnableDelete() {
        return (this.selectedNodes);
      },
      verifyEnableExplain() {
        return (this.selectedNodes && this.selectedNodes[0].isInferred);
      },
      verifyEnableShortestPath() {
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
        this.selectedNodes.forEach((node) => {
          this.visFacade.container.visualiser.deleteNode(node);
        });
        this.$store.commit('selectedNodes', null);
        this.$store.commit('contextMenu', { show: false, x: null, y: null });
      },
      explainNode() {
        this.$store.dispatch(EXPLAIN_CONCEPT).catch((err) => { this.$notifyError(err, 'Explain Concept'); });
        this.$store.commit('contextMenu', { show: false, x: null, y: null });
      },
      computeShortestPath() {
        this.$store.commit('currentQuery', `compute path from "${this.selectedNodes[0].id}", to "${this.selectedNodes[1].id}";`);
        this.$store.dispatch(RUN_CURRENT_QUERY).catch((err) => { this.$notifyError(err, 'Run Query'); });
        this.$store.commit('contextMenu', { show: false, x: null, y: null });
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


