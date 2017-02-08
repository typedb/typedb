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
              <button @click="$emit('load-ontology')" class="btn">Visualise</button>
                <ul class="nav-tabs">
                    <li v-for="k in Object.keys(typeInstances)">
                        <a @click="updateCurrentTab(k)" v-bind:class="[currentTab===k ? 'active noselect' : 'noselect']">{{k[0].toUpperCase() + k.slice(1)}}</a>
                    </li>
                </ul>
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

.active {
    border-bottom: 1px solid #00eca2;
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

.tab-pane{
  margin-top: 5px;
}

a:hover{
  color: #00eca2;
}

.tabs-row {
    display: flex;
    flex-direction: row;
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
