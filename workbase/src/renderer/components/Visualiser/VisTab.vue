<template>
    <div class="visualiser-wrapper">
            <top-bar :tabId="tabId"></top-bar>
            <div class="lower">
                <div class="center">
                    <context-menu :tabId="tabId"></context-menu>
                    <graph-canvas :tabId="tabId"></graph-canvas>
                    <visualiser-footer :tabId="tabId"></visualiser-footer>
                    <!-- <bottom-bar></bottom-bar> -->
                </div>
                <right-bar :tabId="tabId"></right-bar>
            </div>
        </div>
</template>

<style>

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
import TabState from './store/tabState';

export default {
  name: 'VisTab',
  components: {
    TopBar, RightBar, LeftBar, BottomBar, GraphCanvas, ContextMenu, VisualiserFooter,
  },
  props: ['tabId'],
  beforeCreate() {
    this.$store.commit('addTab', { id: this.$options.propsData.tabId, tab: TabState.state() });
  },
};
</script>
