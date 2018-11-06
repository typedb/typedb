<template>
  <div class="graph-panel-body">
    <div id="graph-div" ref="graph"></div>
  </div>
</template>

<style lang="scss" scoped>
  .graph-panel-body {
    height: 100%;
    width: 100%;
    position: absolute;
  }
  .graph-panel-body * {
      -webkit-touch-callout: none;
      -webkit-user-select: none;
      -moz-user-select: none;
      -ms-user-select: none;
      user-select: none;
  }

  #graph-div {
    height: 100%;
  }
</style>

<script>
import { createNamespacedHelpers } from 'vuex';

import VisFacade from '@/components/CanvasVisualiser/Facade';
import { INITIALISE_VISUALISER } from './StoresActions';

export default {
  name: 'GraphCanvas',
  props: ['tabId'],
  beforeCreate() {
    const { mapActions } = createNamespacedHelpers(`tab-${this.$options.propsData.tabId}`);

    // methods
    this.$options.methods = {
      ...(this.$options.methods || {}),
      ...mapActions([INITIALISE_VISUALISER]),
    };
  },
  mounted() {
    this.$nextTick(() => {
      const graphDiv = this.$refs.graph;
      this[INITIALISE_VISUALISER]({ container: graphDiv, visFacade: VisFacade });
    });
  },
};
</script>
