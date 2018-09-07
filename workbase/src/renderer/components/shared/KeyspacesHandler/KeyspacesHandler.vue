<template>
<div class="keyspaces-wrapper">
    <!--<button @click="(isGraknRunning) ? toggleToolTip() : false" :class="{'disabled':!isGraknRunning}" class="btn top-bar-btn">-->
      <!--{{(currentKeyspace !== null) ? currentKeyspace : 'keyspace'}}-->
      <!--<img class="keyspaces-arrow" :src="(toolTipShown === 'keyspaces') ? 'static/img/icons/icon_up_arrow.svg' : 'static/img/icons/icon_down_arrow.svg'">-->
    <!--</button>-->

    <!--<vue-menu>-->
        <!--<div text="hello" intent="primary" key="hello">asdasd</div>-->
    <!--</vue-menu>-->

    <vue-popover :button="keyBtn" :items="keyspaces" v-on:emit-item="setKeyspace"></vue-popover>


    <!--<div @click="(isGraknRunning) ? toggleToolTip() : false"><vue-button rightIcon="key" intent="primary" :text="(currentKeyspace !== null) ? currentKeyspace : 'keyspace'"></vue-button></div>-->

    <!--<transition name="slide-down-fade">-->
        <!--<ul id="keyspaces-list" class="keyspaces-list z-depth-3" v-if="toolTipShown === 'keyspaces'">-->
            <!--<div style="text-align:center;" v-if="keyspaces && !keyspaces.length">no existing keyspace</div>-->
            <!--<li :id="ks" class="ks-key" v-for="ks in keyspaces" :key="ks" @click="setKeyspace(ks)">{{ks}}</li>-->
        <!--</ul>-->
    <!--</transition>-->
</div>
</template>

<style scoped>

li {
    border-bottom: 2px solid rgb(58, 58, 58);
}

li:hover {
    color: #00eca2;
}

.keyspaces-list {
    position: absolute;
    top: 100%;
    margin-top: 5px;
    padding: 5px 10px;
    right:5px;
    background-color: #282828;
    border-radius: 6%;
    z-index: 1;
}

.ks-key {
    position: relative;
    cursor: pointer;
    padding: 10px 5px;
    border-radius: 3px;
    margin: 3px 0px;
}
</style>

<script>

import * as React from 'react';
import { Button } from '@blueprintjs/core';


import { CURRENT_KEYSPACE_CHANGED } from '../StoresActions';

export default {
  name: 'KeyspacesList',
  props: ['localStore', 'toolTipShown'],
  data() {
    return {
      keyspaceItems: [],
      keyBtn: null,
    };
  },
  computed: {
    keyspaces() {
      return this.$store.getters.allKeyspaces;
    },
    currentKeyspace() { return this.localStore.getCurrentKeyspace(); },
    isGraknRunning() { return this.$store.getters.isGraknRunning; },
  },
  created() {
    this.keyBtn = React.createElement(Button, {
      text: (this.currentKeyspace !== null) ? this.currentKeyspace : 'keyspace',
      rightIcon: 'database',
      intent: 'primary',
      className: 'vue-button',
    });
  },
  watch: {
    keyspaces(val) {
      // If user deletes current keyspace from Keyspaces page, set new current keyspace to null
      if (!val.includes(this.currentKeyspace)) { this.localStore.dispatch(CURRENT_KEYSPACE_CHANGED, null); }
    },
    isGraknRunning(val) {
      if (!val) {
        this.$notifyInfo('It was not possible to retrieve keyspaces <br> - make sure Grakn is running <br> - check that host and port in Grakn URI are correct', 'top-center');
      }
    },
    currentKeyspace() {
      this.keyBtn = React.createElement(Button, {
        text: (this.currentKeyspace !== null) ? this.currentKeyspace : 'keyspace',
        rightIcon: 'database',
        intent: 'primary',
        className: 'vue-button',
      });
    },
  },
  methods: {
    setKeyspace(name) {
      this.localStore.dispatch(CURRENT_KEYSPACE_CHANGED, name);
      this.$emit('toggle-tool-tip');
    },
    toggleToolTip() {
      if (!(this.toolTipShown === 'keyspaces')) {
        this.$emit('toggle-tool-tip', 'keyspaces');
      } else {
        this.$emit('toggle-tool-tip');
      }
    },
  },
};
</script>
