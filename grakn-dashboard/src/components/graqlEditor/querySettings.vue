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
along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>. -->


<template>
<div>
    <button @click="showSettings=!showSettings" class="btn btn-default console-button"><i class="fa fa-cog"></i></button>
    <transition name="fade-in">
        <div v-if="showSettings" class="dropdown-content">
            <div class="panel-heading">
                <h4><i class="page-header-icon fa fa-cog"></i>Query settings</h4>
                <a @click="closeSettings"><i class="fa fa-times"></i></a>
            </div>
            <div class="panel-body">
                <div class="dd-item">
                   <div class="left"><input type="checkbox" v-model="useReasoner"></div><div class="right"> Activate inference</div>
                </div>
                <div class="dd-item">
                  <div class="left"><input type="checkbox" v-model="materialiseReasoner" :disabled="!useReasoner"></div><div v-bind:class="[useReasoner ? 'right' : 'right grey']" :disabled="!useReasoner">Materialise inference</div>
                </div>
                <div class="dd-item">
                  <button @click="materialiseAll()" class="btn materialise">Materialise All</button>
                </div>
            </div>
        </div>
    </transition>
</div>
</template>

<style scoped>

.panel-heading {
    padding: 5px 10px;
    display: flex;
    justify-content: space-between;
    margin-bottom: 10px;
}
.left{
  display: inline-flex;
  margin-right: 5px;
}
.right{
  display: inline-flex;
  flex:1;
}
.fa-times{
  cursor: pointer;
}

.grey{
  opacity: 0.5;
}

.page-header-icon {
    font-size: 20px;
    float: left;
    margin-right: 10px;
}

.panel-body {
    display: flex;
    flex-direction: column;
}

.dd-item {
    display: flex;
    flex-direction: row;
    align-items: center;
    flex: 1;
    margin: 10px 5px;
    padding-left: 7px;
}

.list-key{
  display: inline-flex;
  flex:1;
}

.dropdown-content {
    position: absolute;
    top: 100%;
    right: -5%;
    z-index: 2;
    margin-top: 5px;
    background-color: #0f0f0f;
    padding: 10px;
}

.btn.materialise{
  padding: 7px;
  margin:0;
  line-height: 20px;
  margin: auto;
}

</style>

<script>
import User from '../../js/User.js'
import EngineClient from '../../js/EngineClient';

export default {
    name: "QuerySettings",
    data() {
        return {
          useReasoner: User.getReasonerStatus(),
          materialiseReasoner: User.getMaterialiseStatus(),
          showSettings:false,
        }
    },
    created() {},
    mounted: function() {
        this.$nextTick(function() {

        });
    },
    watch: {
        useReasoner: function(newVal, oldVal) {
            User.setReasonerStatus(newVal);
            if(!newVal) this.materialiseReasoner=false;
        },
        materialiseReasoner: function(newVal, oldVal) {
            User.setMaterialiseStatus(newVal);
        }
    },
    methods: {
        closeSettings() {
            this.showSettings = false;
        },
        materialiseAll(){
          EngineClient.preMaterialiseAll();
        }
    }
}
</script>
