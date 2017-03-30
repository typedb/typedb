<!--
Grakn - A Distributed Semantic Database
Copyright (C) 2016  Grakn Labs Limited

Grakn is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

Grakn is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
-->

<template>
<transition name="slideInDown" appear>
    <div v-if="showTypeInstances" class="types-panel">
        <div class="tabs-row z-depth-1">
            <div class="inline-div">
              <button @click="$emit('load-ontology','entity')" class="btn norightmargin">Entities</button>
              <button @click="updateCurrentTab('entities')" :class="{'active':currentTab==='entities'}" class="btn noleftmargin noselect"><i class="fa fa-caret-down"></i></button>
            </div>
            <div class="inline-div">
              <button @click="$emit('load-ontology','resource')" class="btn norightmargin">Resources</button>
              <button @click="updateCurrentTab('resources')" :class="{'active':currentTab==='resources'}" class="btn noleftmargin noselect"><i class="fa fa-caret-down"></i></button>
            </div>
            <div class="inline-div">
              <button @click="$emit('load-ontology','relation')" class="btn norightmargin">Relations</button>
              <button @click="updateCurrentTab('relations')" :class="{'active':currentTab==='relations'}" class="btn noleftmargin noselect"><i class="fa fa-caret-down"></i></button>
            </div>
            <button @click="$emit('load-ontology','concept')" class="btn" style="margin-left:auto;">All Types</button>
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
    props: ['typeInstances', 'showTypeInstances'],
    data: function() {
        return {
            currentTab: undefined,
        }
    },
    mounted: function() {
        this.$nextTick(function() {});
    },
    methods: {
        updateCurrentTab(key) {
            if (this.currentTab === key) {
                this.currentTab = undefined;
            } else {
                this.currentTab = key;
            }
        }
    }
}
</script>
