<template>
    <div class="panel-container">
        <div @click="toggleContent" class="panel-header">
            <vue-icon :icon="(showConceptInfoContent) ?  'chevron-down' : 'chevron-right'" iconSize="14" className="vue-icon"></vue-icon>
            <h1>Identity</h1>
        </div>
        <div v-show="showConceptInfoContent">
            <div class="content noselect" v-if="!currentKeyspace">
                Please select a keyspace
            </div>
            <div class="content noselect" v-else-if="!(nodes && nodes.length === 1)">
                Please select a node
            </div>
            <div class="content" v-else>
                <div class="content-item">
                    <h1 class="label">ID:</h1>
                    <div class="value">{{conceptInfo.id}}</div>
                </div>
                <div class="content-item" v-if="conceptInfo.type">
                    <h1 class="label">TYPE:</h1>
                    <div class="value">{{conceptInfo.type}}</div>
                </div>
                <div class="content-item">
                    <h1 class="label">BASE TYPE:</h1>
                    <div class="value">{{conceptInfo.baseType}}</div>
                </div>
            </div>
        </div>
    </div>
</template>

<script>
  export default {
    name: 'ConceptInfoPanel',
    props: ['localStore'],
    data() {
      return {
        showConceptInfoContent: (this.nodes && this.nodes.length === 1),
      };
    },
    computed: {
      nodes() {
        return this.localStore.getSelectedNodes();
      },
      conceptInfo() {
        if (!this.nodes) return {};
        const node = this.nodes[0];

        if (node.baseType.includes('TYPE')) {
          return {
            id: node.id,
            baseType: node.baseType,
          };
        }
        return {
          id: node.id,
          type: node.type,
          baseType: (node.isInferred) ? `INFERRED_${node.baseType}` : node.baseType,
        };
      },
      currentKeyspace() {
        return this.localStore.getCurrentKeyspace();
      },
    },
    watch: {
      nodes(nodes) {
        this.showConceptInfoContent = nodes && nodes.length === 1;
      },
    },
    methods: {
      toggleContent() {
        this.showConceptInfoContent = !this.showConceptInfoContent;
      },
    },
  };
</script>

<style scoped>

    .content {
        padding: var(--container-padding);
        display: flex;
        flex-direction: column;
        max-height: 80px;
        justify-content: center;
        border-bottom: var(--container-darkest-border);
    }

    .content-item {
        padding: var(--container-padding);
        display: flex;
        flex-direction: row;
    }

    .label {
        margin-right: 20px;
        width: 65px;
    }

    .value {
        width: 110px;
    }

</style>
