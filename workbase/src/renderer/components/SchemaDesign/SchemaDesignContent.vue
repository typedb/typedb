<template>
  <transition name="slide-fade" appear>
    <div class="design-wrapper">
      <top-bar></top-bar>
      <div class="row">
        <left-bar></left-bar>
        <div class="column">
          <context-menu></context-menu>
          <graph-canvas></graph-canvas>
        </div>
        <right-bar></right-bar>
      </div>
    </div>
  </transition>
</template>

<style scoped>

.design-wrapper{
    display: flex;
    flex-direction: column;
    width: 100%;
    display: relative;
}

.row {
  position: relative;
  display: flex;
  flex-direction: row;
  height: 100%;
}

.column {
  position: relative;
  display: flex;
  flex-direction: column;
  width: 100%;
}

.slide-fade-enter-active {
    transition: all .8s ease;
}
.slide-fade-enter,
.slide-fade-leave-active {
    opacity: 0;
}

</style>

<script>

import TopBar from './TopBar';
import GraphCanvas from '../shared/GraphCanvas.vue';
import RightBar from './RightBar';
import LeftBar from './LeftBar';
import ContextMenu from './ContextMenu';

import actions from './store/actions';
import mutations from './store/mutations';
import getters from './store/getters';
import state from './store/state';

export default {
  name: 'SchemaDesignContent',
  components: {
    GraphCanvas, TopBar, RightBar, LeftBar, ContextMenu,
  },
  beforeCreate() {
    this.$store.registerModule('schema-design', { namespaced: true, getters, state, mutations, actions });
  },
};
</script>
