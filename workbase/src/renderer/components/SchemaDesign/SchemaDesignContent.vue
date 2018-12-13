<template>
  <transition name="slide-fade" appear>
    <div class="design-wrapper">
      <top-bar v-on:toggle-preferences="showPreferences = !showPreferences" :showKeyspaceToolTip="showKeyspaceToolTip" v-on:keyspace-selected="showKeyspaceToolTip = false"></top-bar>
      <div class="row">
        <left-bar v-on:keyspace-not-selected="showKeyspaceToolTip = true"></left-bar>
        <div class="column">
          <context-menu></context-menu>
          <graph-canvas></graph-canvas>
          <preferences v-show="showPreferences" v-on:close-preferences="showPreferences = false"></preferences>
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
import Preferences from '../shared/Preferences.vue';
import actions from './store/actions';
import mutations from './store/mutations';
import getters from './store/getters';
import state from './store/state';

export default {
  name: 'SchemaDesignContent',
  components: {
    GraphCanvas, TopBar, RightBar, LeftBar, ContextMenu, Preferences,
  },
  data() {
    return {
      showPreferences: false,
      showKeyspaceToolTip: false,
    };
  },
  beforeCreate() {
    this.$store.registerModule('schema-design', { namespaced: true, getters, state, mutations, actions });
  },
};
</script>
