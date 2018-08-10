<template>
  <transition name="slide-fade" appear> 
    <div class="design-wrapper"> 
        <commands-modal></commands-modal> 
        <menu-bar :localStore="localStore"></menu-bar>
        <div class="design-content">
            <div class="center">
                <node-panel :localStore="localStore"></node-panel>
                <context-menu :localStore="localStore" v-on:open-node-settings="showNodeSettingsPanel=true"></context-menu>
                <graph-canvas :localStore="localStore"></graph-canvas>
                <node-settings-panel 
                    :localStore="localStore" 
                    :showPanel="showNodeSettingsPanel" 
                    v-on:close-panel="showNodeSettingsPanel=false"
                ></node-settings-panel>
            </div>
        </div>
        <div id="cmdBtn" class="cmd">
            <i class="fas fa-info-circle"></i>
        </div>
        <canvas-tool-tip :localStore="localStore"></canvas-tool-tip> 
    </div>
  </transition>
</template>

<style scoped>
.slide-fade-enter-active {
    transition: all .8s ease;
}
.slide-fade-enter,
.slide-fade-leave-active {
    opacity: 0;
}

.design-wrapper{
    display: flex;
    flex-direction: column;
    width: 100%;
}

.design-content {
    display: flex;
    flex-direction: row;
    flex: 1;
    position: relative;
}

.content {
    display: flex;
    flex-direction: row;
    position: relative;
}

.center{
    position: relative;
    display: flex;
    flex-direction: column;
    flex: 1;
}

.cmd{
    padding-left: 20px;
    padding-bottom: 20px;
    font-size: 30px;
    position: absolute;
    bottom: 0;
    left: 0;
    cursor: pointer;
}


</style>

<script>
import { isDataManagement } from '@/routes.js';
import MenuBar from './MenuBar/MenuBar.vue';
import ContextMenu from './Canvas/ContextMenu.vue';
import DataManagementStore from './DataManagementStore';
import GraphCanvas from '../shared/GraphCanvas/GraphCanvas.vue';
import NodeSettingsPanel from './Canvas/NodeSettingsPanel/NodeSettingsPanel.vue';
import CommandsModal from './Canvas/CommandsModal.vue';
import NodePanel from './Canvas/NodePanel';
import CanvasToolTip from './Canvas/CanvasToolTip.vue';

export default {
  name: 'DataManagementContent',
  components: {
    MenuBar, ContextMenu, GraphCanvas, NodeSettingsPanel, NodePanel, CommandsModal, CanvasToolTip,
  },
  data() {
    return {
      localStore: DataManagementStore.create(),
      showNodeSettingsPanel: false,
    };
  },
  computed: {
    keyspaceSelected() {
      return this.localStore.getCurrentKeyspace();
    },
  },
  watch: {
    keyspaceSelected() {
      if (this.keyspaceSelected) {
        clearInterval(this.notifyNoKeyspace);
      }
    },
  },
  mounted() {
    if (!this.keyspaceSelected) {
      this.notifyNoKeyspace = setTimeout(() => {
        if (isDataManagement(this.$route) && this.$store.getters.isGraknRunning) {
          this.$notifyInfo('Please select a keyspace to start exploring data!');
        }
      }, 10000);
    }
  },
};
</script>
