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
<transition name="slideInDown" appear>
    <div v-if="showTypeInstances" class="types-panel">
        <div class="tabs-row z-depth-1">
            <div class="inline-div">
              <button @click="$emit('load-schema','entity')" class="btn norightmargin">Entities</button>
              <button @click="updateCurrentTab('entities')" :class="{'active':currentTab==='entities'}" class="btn noleftmargin noselect"><i class="fa fa-caret-down"></i></button>
            </div>
            <div class="inline-div">
              <button @click="$emit('load-schema','attribute')" class="btn norightmargin">Attributes</button>
              <button @click="updateCurrentTab('attributes')" :class="{'active':currentTab==='attributes'}" class="btn noleftmargin noselect"><i class="fa fa-caret-down"></i></button>
            </div>
            <div class="inline-div">
              <button @click="$emit('load-schema','relationship')" class="btn norightmargin">Relationships</button>
              <button @click="updateCurrentTab('relationships')" :class="{'active':currentTab==='relationships'}" class="btn noleftmargin noselect"><i class="fa fa-caret-down"></i></button>
            </div>
            <button @click="$emit('load-schema','thing')" class="btn" style="margin-left:auto;">All Types</button>
        </div>
        <transition name="fade-in" v-for="k in Object.keys(typeInstances)">
            <div v-bind:id="k+'-tab'" class="tab-pane" v-show="currentTab===k">
                <div v-bind:class="k+'-group concepts-group'">
                    <div v-for="i in typeInstances[k]">
                        <button @click="$emit('type-query',k, i)" class="btn btn-link">{{i}}</button>
                    </div>
                </div>
            </div>
        </transition>
    </div>
</transition>
</template>

<style scoped>
a,
button {
    cursor: pointer;
}

i{
  font-size: 90%;
}

.norightmargin{
  margin-right: 0px;
  border-top-right-radius: 0px;
  border-bottom-right-radius: 0px;
  border-right: 1px solid #606060;
}
.noleftmargin{
  margin-left: 0px;
  border-top-left-radius: 0px;
  border-bottom-left-radius: 0px;
  border-left: 1px solid #606060;
}

.inline-div{
  display: inline-flex;
}

.active {
    background-color: #00eca2;
    opacity: 0.9;
}

.types-panel {
    position: absolute;
    top: 100%;
    margin-top: 10px;
    display: flex;
    width: 100%;
    flex-direction: column;
    z-index: 0;
}


.tab-pane {
    margin-top: 5px;
}

a:hover {
    color: #00eca2;
}

.tabs-row {
    display: flex;
    flex-direction: row;
    justify-content: start;
    background-color: #0f0f0f;
}

.concepts-group {
    display: flex;
    flex-direction: row;
    flex-wrap: wrap;
}

.nav-tabs {
    width: 100%;
    display: flex;
    justify-content: space-around;
    flex-direction: row;
    align-items: center;
    flex: 1;
}
</style>
<script>
export default {
  name: 'TypeInstacesPanel',
  props: ['typeInstances', 'showTypeInstances','state'],
  created(){
  },
  data() {
    return {
      currentTab: undefined,
    };
  },
  mounted() {
    this.$nextTick(() => {    });
  },
  methods: {
    updateCurrentTab(key) {
      if (this.currentTab === key) {
        this.currentTab = undefined;
      } else {
        this.currentTab = key;
      }
    },
  },
};
</script>
