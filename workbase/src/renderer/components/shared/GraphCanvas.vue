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
import { INITIALISE_VISUALISER } from './StoresActions';

export default {
  name: 'WorkbaseCanvas',
  data() {
    return {
      init: false,
    };
  },
  mounted() {
    this.$nextTick(() => {
      if (this.init) return;
      const graphDiv = this.$refs.graph;
      this.$store.dispatch(INITIALISE_VISUALISER, graphDiv);
      this.$emit('init');
      this.init = true;
    });
  },
};
</script>
