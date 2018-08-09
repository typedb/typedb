<template>
  <transition name="slide-fade" appear> 
    <div v-show="showNodePanel" id="node-panel" class="node-panel z-depth-3">
      <div class="panel-body">
        <div class="dd-header">Node:<i @click="showNodePanel=false" class="fa fa-times"></i>
        </div>
        <div class="node-features">
          <div class="dd-item" v-for="(value, key) in nodeProperties" :key="key">
            <div><span class="list-key">{{key}}:</span> {{value}}</div>
          </div>
        </div>
        <div class="dd-header" v-show="attributes.length">Attributes:</div>
        <div class="node-features">
          <div class="dd-item" v-for="(value, key) in attributes" :key="key">
            <div><span class="list-key">{{value.type}}:</span> {{value.value}}</div>
          </div>
        </div>
      </div>
    </div>
  </transition>
</template>
<script>
export default {
  name: 'NodePanel',
  props: ['localStore'],
  data() {
    return {
      showNodePanel: false,
      attributes: [],
    };
  },
  computed: {
    node() {
      return this.localStore.getSelectedNodes();
    },
    nodeProperties() {
      const node = this.localStore.getSelectedNode();
      if (!node) return {};

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
    async node(val) {
      // If no node selected: close panel and return
      if (!val || val.length > 1) { this.showNodePanel = false; return; }
      const node = await this.localStore.getNode(val[0].id);
      // else compute new attributes
      const attributes = (node.isSchemaConcept()) ? [] : await Promise.all((await (await node.attributes()).collect()).map(async x => ({
        type: await (await x.type()).label(),
        value: await x.value(),
      })));
      this.attributes = Object.values(attributes).sort((a, b) => ((a.type > b.type) ? 1 : -1));
      if (val.length === 1) this.showNodePanel = true;
    },
  },
};
</script>
<style scoped>
.node-panel {
  position: absolute;
  top: 1%;
  right: 1%;
  background-color: #282828;
  padding-right: 15px;
  padding-left: 15px;
  padding-bottom: 15px;
  padding-top: 5px;
  max-height: 95%;
  border-radius: 1%;
  z-index: 1;
  width: 300px;
}

.fa-times{
  cursor: pointer;
  float: right;
}

.dd-header {
    margin-top: 10px;
    margin-bottom: 5px;
    background-color: #363636;
}

.dd-item {
    padding: 5px 10px;
    line-height: 1.2em;
}

.node-panel .panel-body {
    overflow: scroll;
}

.node-panel .panel-body::-webkit-scrollbar {
    display: none;
}

.list-key {
    font-weight: bolder;
    color: #706f6f;
}

.slide-fade-enter-active {
    transition: all .6s ease;
}
.slide-fade-enter,
.slide-fade-leave-active {
    transform: translateX(10px);
    opacity: 0;
}
</style>


