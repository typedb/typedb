<template>
  <div class="left-bar-container">
    <new-entity-panel :panelShown="panelShown" v-on:show-panel="togglePanel"></new-entity-panel>
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

  import NewEntityPanel from './LeftBar/NewEntityPanel';

  export default {
    components: { NewEntityPanel },
    data() {
      return {
        panelShown: undefined,
      };
    },
    methods: {
      togglePanel(panel) {
        if (this.panelShown) this.panelShown = undefined;
        else this.panelShown = panel;
      },
    },
  };
</script>
