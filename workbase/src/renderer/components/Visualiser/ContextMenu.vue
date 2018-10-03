<template>
    <div v-show="showContextMenu" id="context-menu" class="z-depth-2">
        <li @click="(contextMenuOptions.enableDelete) ? deleteNode() : false" class="context-action" :class="{'disabled':!contextMenuOptions.enableDelete}">Delete</li>
        <li @click="(contextMenuOptions.enableExplain) ? explainNode() : false" class="context-action" :class="{'disabled':!contextMenuOptions.enableExplain}">Explain</li>
        <li @click="(contextMenuOptions.enableShortestPath) ? computeShortestPath() : false" class="context-action" :class="{'disabled':!contextMenuOptions.enableShortestPath}">Shortest Path</li>
    </div>
</template>
<script>
  import { RUN_CURRENT_QUERY, EXPLAIN_CONCEPT } from '@/components/shared/StoresActions';
  import { mapGetters } from 'vuex';


  export default {
    name: 'ContextMenu',
    computed: {
      ...mapGetters(['selectedNodes', 'currentKeyspace', 'showContextMenu', 'contextMenuOptions', 'visFacade']),
    },
    methods: {
      deleteNode() {
        this.selectedNodes.forEach((node) => {
          this.visFacade.container.visualiser.deleteNode(node);
        });
        this.$store.commit('selectedNodes', null);
        this.$store.commit('showContextMenu', false);
      },
      explainNode() {
        this.$store.dispatch(EXPLAIN_CONCEPT).catch((err) => { this.$notifyError(err, 'Explain Concept'); });
        this.$store.commit('showContextMenu', false);
      },
      computeShortestPath() {
        this.$store.commit('currentQuery', `compute path from "${this.selectedNodes[0].id}", to "${this.selectedNodes[1].id}";`);
        this.$store.dispatch(RUN_CURRENT_QUERY).catch((err) => { this.$notifyError(err, 'Run Query'); });
        this.$store.commit('showContextMenu', false);
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


