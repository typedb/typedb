<template>
  <transition name="slide-fade">
    <div class="canvas-tool-tip" v-if="currentKeyspace">
      <div id="nodes"> Nodes: {{nodes}}</div>
      <div id="edges"> Edges: {{edges}}</div>
    </div>
  </transition>
</template>

<style scoped>

.canvas-tool-tip {
  position: absolute;
  bottom: 20px;
  right: 20px;
  background-color: #282828;
  padding: 5px 5px;
  display: flex;
  flex-direction: column;
  z-index: 1;
}
</style>

<script>

export default {
  name: 'CanvasToolTip',
  props: ['localStore'],
  data() {
    return {
      nodes: 0,
      edges: 0,
    };
  },
  computed: {
    currentKeyspace() {
      return this.localStore.getCurrentKeyspace();
    },
    getData() {
      return this.localStore.canvasData;
    },
  },
  watch: {
    getData(cavasData) {
      this.nodes = cavasData.nodes;
      this.edges = cavasData.edges;
    },
  },
};
</script>
