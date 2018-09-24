<template>
    <transition name="fade" appear>
        <div class="visualiser-wrapper">
            <top-bar :localStore="localStore"></top-bar>
            <div class="lower">
                <div class="center">
                    <context-menu :localStore="localStore"></context-menu>
                    <graph-canvas :localStore="localStore"></graph-canvas>
                    <canvas-tool-tip :localStore="localStore"></canvas-tool-tip>
                    <!--<bottom-bar :localStore="localStore"></bottom-bar>-->
                    <right-bar :localStore="localStore"></right-bar>
                </div>
            </div>
        </div>
    </transition>
</template>

<style scoped>


    .visualiser-wrapper {
        display: flex;
        flex-direction: column;
        width: 100%;
        height: 100%;
        position: absolute;
        background-color: #1B1B1B;
    }

    .lower {
        display: flex;
        flex-direction: row;
        flex: 1;
        position: relative;
    }

    .center {
        display: flex;
        flex-direction: column;
        justify-content: flex-end;
        width: 100%;
        position: relative;
    }

</style>

<script>

import TopBar from './TopBar.vue';
import LeftBar from './LeftBar.vue';
import RightBar from './RightBar.vue';
import BottomBar from './BottomBar.vue';

import VisualiserStore from './VisualiserStore';
import GraphCanvas from '../shared/GraphCanvas.vue';
import ContextMenu from './ContextMenu';
import CanvasToolTip from './Footer';


export default {
  name: 'VisualiserContent',
  components: {
    TopBar, LeftBar, RightBar, BottomBar, GraphCanvas, ContextMenu, CanvasToolTip,
  },
  data() {
    return {
      localStore: VisualiserStore.create(),
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
  methods: {
    toggleRightBar() {
      this.showRightBar = !this.showRightBar;
    },
  },
};
</script>
