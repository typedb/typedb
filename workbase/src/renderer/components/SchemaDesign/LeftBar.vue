<template>
  <div class="left-bar-container">
    <new-entity-panel :panelShown="panelShown" v-on:show-panel="togglePanel"></new-entity-panel>
    <new-attribute-panel :panelShown="panelShown" v-on:show-panel="togglePanel"></new-attribute-panel>
  </div>
</template>

<style scoped>

  .left-bar-container {
    background-color: var(--gray-3);
    border-right: var(--container-darkest-border);
    height: 100%;
    position: relative;
    z-index: 1;
    padding: var(--container-padding);
    display: flex;
    align-items: center;
    flex-direction: column;
  }

</style>

<script>
  import { createNamespacedHelpers } from 'vuex';

  import NewEntityPanel from './LeftBar/NewEntityPanel';
  import NewAttributePanel from './LeftBar/NewAttributePanel';


  export default {
    components: { NewEntityPanel, NewAttributePanel },
    data() {
      return {
        panelShown: undefined,
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
    methods: {
      togglePanel(panel) {
        if (!this.currentKeyspace) this.$emit('keyspace-not-selected');
        else this.panelShown = panel;
      },
    },
  };
</script>
