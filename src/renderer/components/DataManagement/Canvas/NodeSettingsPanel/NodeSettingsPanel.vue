<template>
  <transition name="slide-fade">
      <div class="panel-wrapper  z-depth-3" v-show="showPanel && attributesLoaded" ref="panel">
        <div><i class="fa fa-times" @click="closePanel()"></i></div>
        <div class="modal-title">
          <div v-if="node">{{node.type}} node settings  
          </div>
        </div>
        <div class="panel-body">
          <div class="sub-title" v-if="showProperties">
            <p v-if="!nodeAttributes.length">There is no attribute type available for this type of node.</p>
          </div>
          <div class="properties-list" v-if="showProperties">
            <ul class="dd-list">
              <li class="dd-item"  @click="toggleAttributeToLabel(prop)" v-for="prop in nodeAttributes" :key=prop :class="{'li-active':currentTypeSavedAttributes.includes(prop)}">
                {{prop}}
              </li>
              <button v-if="nodeAttributes.length" class="dd-item reset-label-btn" @click="toggleAttributeToLabel()"><i class="fas fa-sync-alt"></i></button>
            </ul>
          </div>
          <div class="side-column">
            <colour-picker :localStore="localStore"></colour-picker>
          </div>
        </div>
      </div>
  </transition>
</template>

<style scoped>

.fa-times {
  float: right;
  cursor: pointer;
}

.fa-times:hover{
  color: #06b17b;
}

.panel-wrapper{
  position: absolute;
  bottom: 10px;
  left: 10px;
  background-color: #282828;
  padding: 10px 20px;
  display: flex;
  flex-direction: column;
  z-index: 1;
  width: 200px;
}

.modal-title{
  background-color: #363636;
  margin-bottom: 10px;
  font-weight: bolder;
}

.sub-title{
  font-size:85%;
}

.panel-body{
  display: flex;
  flex-direction: row;
  margin-bottom: 5px;
  margin-top: 10px;
}

.side-column{
  display: flex;
  flex: 1;
}

.reset-label-btn {
  background-color: #282828;
}
.reset-label-btn:hover {
  background-color: #3d404c;
}

.li-active{
  background-color: #3d404c;
  color: white;
}

li:hover{
  background-color: #3d404c;
}

.properties-list{
  display: flex;
  flex:4;
  justify-content: center;
}

.dd-list{
  width: 100%;
} 

.dd-item{
  color:white;
  cursor: pointer;
  padding: 5px;
  border: 1px solid #3d404c;
  border-radius: 3px;
  margin: 2px 0px;
  width: 100%;
  font-size: 110%;
}

.slide-fade-enter-active {
    transition: all .6s ease;
}

.slide-fade-enter,
.slide-fade-leave-active {
    transform: translateY(10px);
    opacity: 0;
}

</style>

<script>
import { TOGGLE_LABEL } from '@/components/shared/StoresActions';
import NodeSettings from './NodeSettings';
import ColourPicker from './ColourPicker.vue';

export default {
  name: 'NodeSettingsPanel',
  props: ['localStore', 'showPanel'],
  components: {
    ColourPicker,
  },
  data() {
    return {
      nodeAttributes: [],
      currentTypeSavedAttributes: [],
      attributesLoaded: false,
      showProperties: true,
    };
  },
  computed: {
    node() {
      let node = this.localStore.getSelectedNode();

      if (node) {
        if (node.baseType.includes('Type')) {
          node = null;
        }
      }
      return node;
    },
  },
  watch: {
    node() { // when user clicks on another node close panel
      this.$emit('close-panel');
      this.nodeAttributes = [];
    },
    showPanel(open) {
      if (open) {
        this.attributesLoaded = false;
        if (this.node.type) this.loadAttributeTypes().then(() => { this.attributesLoaded = true; this.showProperties = true; });
        else this.attributesLoaded = true; this.showProperties = false;
      }
    },
  },
  methods: {
    async loadAttributeTypes() {
      const node = await this.localStore.getNode(this.node.id);
      const type = await node.type();
      this.nodeAttributes = await Promise.all((await (await type.attributes()).collect()).map(type => type.label()));
      this.nodeAttributes.sort();
      this.currentTypeSavedAttributes = this.localStore.getLabelBySelectedType();
    },
    toggleAttributeToLabel(attribute) {
      // Persist changes into localstorage for current type
      NodeSettings.toggleLabelByType({ type: this.node.type, attribute });
      this.localStore.dispatch(TOGGLE_LABEL, this.node.type);
      this.currentTypeSavedAttributes = this.localStore.getLabelBySelectedType();
    },
    closePanel() { this.$emit('close-panel'); this.attributesLoaded = false; this.attributes = []; },
    repositionPanel() { // This is not used, TODO: decide if we need it after discussion with designers.
      const visualiserNetwork = this.localStore.visFacade.container.visualiser.getNetwork();
      const nodePosition = visualiserNetwork.getPositions(this.node.id)[this.node.id];
      const nodeBox = visualiserNetwork.getBoundingBox(this.node.id);
      const nodeDOMPosition = visualiserNetwork.canvasToDOM(nodePosition);

      const panel = this.$refs.panel;
      const topMargin = ((nodeBox.top - nodeBox.bottom) / 2);
      panel.style.left = `${nodeDOMPosition.x - 125}px`;
      panel.style.top = `${nodeDOMPosition.y - topMargin - (topMargin * 0.3)}px`;
    },
  },
};
</script>
