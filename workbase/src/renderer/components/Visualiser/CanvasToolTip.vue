<template>
    <div class="canvas-tool-tip noselect" v-if="currentKeyspace">
        <div class="canvas-data">
            <div id="nodes"> nodes: {{nodes}}</div>
            <div id="edges"> edges: {{edges}}</div>
        </div>
    </div>
</template>

<style scoped>

    .canvas-data {

    }

.canvas-tool-tip {
    border-top: var(--container-darkest-border);
    justify-content: flex-end;
  background-color: #282828;
  padding: var(--container-padding);
  display: flex;
  flex-direction: row;
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
