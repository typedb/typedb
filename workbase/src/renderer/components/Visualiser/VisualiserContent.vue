<template>
    <transition name="fade" appear>
        <div class="visualiser-wrapper">
            <top-bar v-on:toggle-preferences="showPreferences = !showPreferences"></top-bar>
            <preferences v-show="showPreferences"  v-on:close-preferences="showPreferences = !showPreferences"></preferences>
            <div class="lower">
                <div class="center">
                    <context-menu></context-menu>
                    <graph-canvas></graph-canvas>
                    <visualiser-footer></visualiser-footer>
                    <!--<bottom-bar></bottom-bar>-->
                    <right-bar></right-bar>
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

import GraphCanvas from '../shared/GraphCanvas.vue';
import ContextMenu from './ContextMenu';
import VisualiserFooter from './VisualiserFooter';
import Preferences from './Preferences';


export default {
  name: 'VisualiserContent',
  components: {
    TopBar, LeftBar, RightBar, BottomBar, GraphCanvas, ContextMenu, VisualiserFooter, Preferences,
  },
  data() {
    return {
      showPreferences: false,
    };
  },
  methods: {
    toggleRightBar() {
      this.showRightBar = !this.showRightBar;
    },
  },
};
</script>
