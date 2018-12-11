<template>
<div>
  <div class="left-bar-container noselect">
    <div class="panel-header">
      <h1>Define</h1>
    </div>
    <div class="content">
      <new-entity-panel :showPanel="showPanel" v-on:show-panel="togglePanel"></new-entity-panel>
      <new-attribute-panel :showPanel="showPanel" v-on:show-panel="togglePanel"></new-attribute-panel>
      <new-relationship-panel :showPanel="showPanel" v-on:show-panel="togglePanel"></new-relationship-panel>
    </div>
  </div>
</div>
</template>

<style scoped>

  .left-bar-container {
    background-color: var(--gray-3);
    border-right: var(--container-darkest-border);
    height: 100%;
    position: relative;
    z-index: 1;
    display: flex;
    align-items: center;
    flex-direction: column;
  }

  .content { 
      display: flex;
      flex-direction: column;
      padding: var(--container-padding);
  }

  .panel-header {
    justify-content: center;
    cursor: default;
  }

</style>

<script>
  import { createNamespacedHelpers } from 'vuex';

  import NewEntityPanel from './LeftBar/NewEntityPanel';
  import NewAttributePanel from './LeftBar/NewAttributePanel';
  import NewRelationshipPanel from './LeftBar/NewRelationshipPanel';

  export default {
    components: { NewEntityPanel, NewAttributePanel, NewRelationshipPanel },
    data() {
      return {
        showPanel: undefined,
      };
    },
    beforeCreate() {
      const { mapGetters } = createNamespacedHelpers('schema-design');

      // computed
      this.$options.computed = {
        ...(this.$options.computed || {}),
        ...mapGetters(['currentKeyspace']),
      };
    },
    watch: {
      currentKeyspace() {
        this.showPanel = undefined;
      },
    },
    methods: {
      togglePanel(panel) {
        if (!this.currentKeyspace) this.$emit('keyspace-not-selected');
        else this.showPanel = panel;
      },
    },
  };
</script>
