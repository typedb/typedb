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
import VisFacade from '@/components/CanvasVisualiser/Facade';
import { INITIALISE_VISUALISER } from './StoresActions';

export default {
  name: 'GraphCanvas',
  props: ['tabId'],
  mounted() {
    this.$nextTick(() => {
      const graphDiv = this.$refs.graph;
      this.$store.dispatch(INITIALISE_VISUALISER, { id: this.tabId, container: graphDiv, visFacade: VisFacade });
    });
  },
};
</script>
