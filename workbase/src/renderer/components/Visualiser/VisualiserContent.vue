<template>
    <transition name="fade" appear>
        <div class="visualiser-wrapper">
            <top-bar :localStore="localStore"></top-bar>
            <div class="lower">
                <!--<left-bar></left-bar>-->
                <div class="center">
                    <context-menu :localStore="localStore"></context-menu>
                    <graph-canvas :localStore="localStore"></graph-canvas>
                    <bottom-bar :localStore="localStore"></bottom-bar>
                </div>
                <right-bar :localStore="localStore"></right-bar>
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
    background-color: var(--border-darkest-color);
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
import { isDataManagement } from '@/routes.js';

import TopBar from './TopBar.vue';
import LeftBar from './LeftBar.vue';
import RightBar from './RightBar.vue';
import BottomBar from './BottomBar.vue';

import VisualiserStore from './VisualiserStore';
import GraphCanvas from '../shared/GraphCanvas/GraphCanvas.vue';
import ContextMenu from './ContextMenu';


export default {
  name: 'VisualiserContent',
  components: {
    TopBar, LeftBar, RightBar, BottomBar, GraphCanvas, ContextMenu,
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
