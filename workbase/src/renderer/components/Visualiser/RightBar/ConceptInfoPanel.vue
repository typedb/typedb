<template>
    <div class="panel-container">
        <div @click="toggleContent" class="panel-header" v-bind:style="{ opacity: (nodes && nodes.length === 1) ? 1 : 0.5 }">
            <vue-icon :icon="(showConceptInfoContent) ?  'chevron-down' : 'chevron-right'" iconSize="14"></vue-icon>
            <h1>CONCEPT INFO</h1>
        </div>
        <div class="content" v-show="showConceptInfoContent">
            <div class="content-item">
                <h1 class="label">ID</h1>
                <div class="value">{{conceptInfo.id}}</div>
            </div>
            <div class="content-item">
                <h1 class="label">TYPE</h1>
                <div class="value">{{conceptInfo.type}}</div>
            </div>
            <div class="content-item">
                <h1 class="label">BASE TYPE</h1>
                <div class="value">{{conceptInfo.baseType}}</div>
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
        showConceptInfoContent: false,
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
          baseType: (node.explanation && node.explanation.answers().length) ? 'INFERRED_RELATION' : node.baseType,
        };
      },
    },
    watch: {
      nodes(nodes) {
        this.showConceptInfoContent = nodes && nodes.length === 1;
      },
    },
    methods: {
      toggleContent() {
        if (this.nodes && this.nodes.length === 1) {
          this.showConceptInfoContent = !this.showConceptInfoContent;
        }
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

</style>
