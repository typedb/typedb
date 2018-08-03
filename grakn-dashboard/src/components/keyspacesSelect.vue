<!--
GRAKN.AI - THE KNOWLEDGE GRAPH
Copyright (C) 2018 Grakn Labs Ltd

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
-->

<template>
<div class="keyspaces-wrapper">
    <div><span>Keyspace: </span></div>
    <div class="dropdown">
        <div @click="showList=!showList" class="selector-button noselect">{{currentKeyspace}}</div>
        <transition name="fade-in">
            <ul class="keyspaces-list z-depth-1-half" v-show="showList">
                <li class="ks-key" v-for="ks in keyspaces" @click="setKeySpace(ks)">{{ks}}</li>
            </ul>
        </transition>
    </div>
</div>
</template>

<style scoped>
.keyspaces-wrapper {
    display: inline-flex;
    align-items: center;
}

.selector-button {
    cursor: pointer;
    color: #00eca2;
    border-bottom: 1px solid #00eca2;
    display: flex;
    justify-content: flex-end;
    padding: 5px;
    margin-left: 2px;
    font-weight: 400;
    font-size: 130%;
}

li:hover {
    color: #00eca2;
}

.keyspaces-list {
    position: absolute;
    top: 100%;
    background-color: #0f0f0f;
    margin-top: 5px;
    width: 150px;
    padding: 5px 10px;
    right: 3px;
}

.ks-key {
    cursor: pointer;
    border: 1px solid #3d404c;
    padding: 10px 10px;
    border-radius: 3px;
    margin: 3px 0px;
}
</style>
<script>
import User, {
    DEFAULT_KEYSPACE,
} from '../js/User.js';
import EngineClient from '../js/EngineClient.js';
import GraphPageState from '../js/state/graphPageState';
import ConsolePageState from '../js/state/consolePageState';


export default {
  name: 'KeyspacesSelect',
  data() {
    return {
      keyspaces: [],
      currentKeyspace: undefined,
      showList: false,
      state: undefined,
    };
  },

  created() {
    this.loadState();
  },
  watch: {
        // When the route changes we need to check which state we need to emit the events to.
    $route(newRoute) {
      this.loadState();
    },
  },
  mounted() {
    this.$nextTick(function () {
      EngineClient.fetchKeyspaces().then((res, err) => {
        const list = JSON.parse(res).keyspaces;

        this.currentKeyspace = User.getCurrentKeySpace();

        for (let i = 0; i < list.length; i++) {
          this.keyspaces.push(list[i].name);
        }
      });
    });
  },
  methods: {
    loadState() {
      switch (this.$route.fullPath) {
        case '/console':
          this.state = ConsolePageState;
          break;
        case '/graph':
          this.state = GraphPageState;
          break;
      }
    },
    setKeySpace(name) {
      User.setCurrentKeySpace(name);
      this.currentKeyspace = name;
      this.state.eventHub.$emit('keyspace-changed');
      this.showList = false;
    },

  },
};
</script>
