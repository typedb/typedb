<template>
  <transition name="fade" appear> 
    <div class="visulaiser-wrapper"> 
      <top-bar></top-bar>
      <div class="center">
        <left-bar></left-bar>
        <div class="bottom">
          <bottom-bar></bottom-bar>
        </div>
        <right-bar></right-bar>
      </div>
    </div>





    <!-- <div class="design-wrapper"> 
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
    </div> -->
  </transition>
</template>

<style scoped>
.visulaiser-wrapper {
  display: flex;
  flex-direction: column;
  width: 100%;
  height: 100%;
  position: absolute;
}

.center {
  display: flex;
  flex: 1;
  flex-direction: row;
  position: relative;
}

.bottom {
  display: flex;
  flex-direction: column;
  justify-content: flex-end;
  width: 100%;
}








/* .slide-fade-enter-active {
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
} */


</style>

<script>
import { isDataManagement } from '@/routes.js';
import TopBar from './TopBar.vue';
import LeftBar from './LeftBar.vue';
import RightBar from './RightBar.vue';
import BottomBar from './BottomBar.vue';

import ContextMenu from './ContextMenu.vue';
import DataManagementStore from '../DataManagementStore';
import GraphCanvas from '../../shared/GraphCanvas/GraphCanvas.vue';
import NodeSettingsPanel from './NodeSettingsPanel/NodeSettingsPanel.vue';
import CommandsModal from './CommandsModal.vue';
import NodePanel from './NodePanel';
import CanvasToolTip from './CanvasToolTip.vue';

export default {
  name: 'DataManagementContent',
  components: {
    TopBar, LeftBar, RightBar, BottomBar, ContextMenu, GraphCanvas, NodeSettingsPanel, NodePanel, CommandsModal, CanvasToolTip,
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
