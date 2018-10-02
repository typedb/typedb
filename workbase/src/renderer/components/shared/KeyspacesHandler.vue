<template>
<div class="keyspaces-wrapper">

    <div class="keyspaces" @click="toggleKeyspaceList"><vue-tooltip class="keyspace-tooltip" content="Please select a keyspace" :isOpen="showKeyspaceTooltip" :child="keyspaceBtn"></vue-tooltip></div>

        <ul id="keyspaces-list" class="keyspaces-list arrow_box z-depth-1" v-if="showKeyspaceList">
            <div style="text-align:center;" v-if="keyspaces && !keyspaces.length">no existing keyspace</div>
            <li :id="ks" v-bind:class="(ks === currentKeyspace)? 'ks-key active noselect' : 'ks-key noselect'" v-for="ks in keyspaces" :key="ks" @click="setKeyspace(ks)">{{ks}}</li>
        </ul>
</div>
</template>

<style scoped>

    .arrow_box {
        position: relative;
        background: var(--gray-1);
        border: var(--container-darkest-border);
    }
    .arrow_box:after, .arrow_box:before {
        bottom: 100%;
        left: 50%;
        border: solid transparent;
        content: " ";
        height: 0;
        width: 0;
        position: absolute;
        pointer-events: none;
    }

    .arrow_box:after {
        border-bottom-color: var(--gray-1);
        border-width: 10px;
        margin-left: -10px;
    }
    .arrow_box:before {
        border-bottom-color: var(--border-darkest-color);
        border-width: 11px;
        margin-left: -11px;
    }






.keyspaces-list {
    position: absolute;
    top: 100%;
    margin-top: 5px;
    /*padding: 5px 10px;*/
    right:5px;
    background-color: #282828;
    z-index: 1;
    min-width: 100px;
    max-width:  130px;
    word-break: break-word;
    background-color: var(--gray-1);
    border: var(--container-darkest-border);
}

/*dynamic class*/

    .ks-key {
        position: relative;
        cursor: pointer;
        padding: 5px;
        min-height: 22px;

    }

    .ks-key:hover {
        background-color: var(--purple-4);
    }

</style>

<script>

import * as React from 'react';
import { Button } from '@blueprintjs/core';
import storage from '@/components/shared/PersistentStorage';


import { CURRENT_KEYSPACE_CHANGED } from './StoresActions';

export default {
  name: 'KeyspacesList',
  props: ['localStore', 'showKeyspaceTooltip'],
  data() {
    return {
      keyspaceItems: [],
      keyBtn: null,
      showKeyspaceList: false,
      keyspaceBtn: null,
      clickEvent: () => {
        this.showKeyspaceList = false;
      },
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
    this.renderButton();
  },
  watch: {
    keyspaces(val) {
      // If user deletes current keyspace from Keyspaces page, set new current keyspace to null
      if (!val.includes(this.currentKeyspace)) { this.localStore.dispatch(CURRENT_KEYSPACE_CHANGED, null); }
    },
    isGraknRunning(val) {
      if (!val) {
        this.$notifyError('It was not possible to retrieve keyspaces <br> - make sure Grakn is running <br> - check that host and port in connection settings are correct');
      }
    },
    currentKeyspace() {
      this.renderButton();
    },
    showKeyspaceTooltip() {
      this.renderButton();
    },
    showKeyspaceList(show) {
      // Close keyspaces list when user clicks anywhere else
      if (show) window.addEventListener('click', this.clickEvent);
      else window.removeEventListener('click', this.clickEvent);

      this.renderButton();
    },
  },
  methods: {
    setKeyspace(name) {
      this.$emit('keyspace-selected');
      storage.set('current_keyspace_data', name);

      this.localStore.dispatch(CURRENT_KEYSPACE_CHANGED, name);
      this.showKeyspaceList = false;
    },
    renderButton() {
      let text;
      if (this.currentKeyspace !== null) {
        if (this.currentKeyspace.length > 15) { // truncate long keyspace names
          text = `${this.currentKeyspace.substring(0, 15)}...`;
        } else {
          text = this.currentKeyspace;
        }
      } else {
        text = 'keyspace';
      }

      this.keyspaceBtn = React.createElement(Button, {
        text,
        rightIcon: 'database',
        intent: 'primary',
        className: (this.showKeyspaceList) ? 'vue-button keyspace-btn' : 'vue-button',
      });
    },
    toggleKeyspaceList() {
      this.$emit('keyspace-selected');
      this.showKeyspaceList = !this.showKeyspaceList;
    },
  },
};
</script>
