<template>
    <div class="picker-column">
        <div class="selector-square red" @click="configureColor('#C84343')"></div>
        <div class="selector-square blue" @click="configureColor('#0CA1CF')"></div>
        <div class="selector-square green" @click="configureColor('#279d5d')"></div>
        <div class="selector-square yellow" @click="configureColor('#b3a41e')"></div>
        <button class="reset-color" @click="configureColor()"><i class="fas fa-sync-alt"></i></button>
    </div>
</template>

<style scoped>
.selector-square{
  height: 30px;
  width: 30px;
  cursor: pointer;
  margin-bottom: 5px;
}

.default{
  border: 1px solid white;
  color: white;
}

.reset-color{
    color:white;
  cursor: pointer;
  padding: 5px;
  border: 1px solid #3d404c;
  border-radius: 3px;
  margin: 2px 0px;
  width: 100%;
  font-size: 110%;
  background-color: #282828;
}
.reset-color:hover {
  background-color: #3d404c;
}

.red{
  background-color: #ff7878;
}

.blue{
  background-color:  #0CA1CF;
}
.green{
  background-color: #279d5d;
}

.yellow{
  background-color: #b3a41e;
}

.picker-column{
  display: flex;
  flex-direction: column;
  margin-left: 20%;
  margin-top: 3px;
}

</style>

<script>

import { TOGGLE_COLOUR } from '@/components/shared/StoresActions';
import NodeSettings from './NodeSettings';

export default {
  name: 'ColourPicker',
  props: ['localStore'],
  computed: {
    node() {
      return this.localStore.getSelectedNode();
    },
  },
  methods: {
    configureColor(colourString) {
      // Persist changes into localstorage for current colour
      NodeSettings.toggleColourByType({ type: this.node.type, colourString });
      this.localStore.dispatch(TOGGLE_COLOUR, this.node.type);
    },
  },
};
</script>
