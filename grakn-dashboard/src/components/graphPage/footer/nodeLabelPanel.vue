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
<transition name="slideInUp">
    <div class="panel-wrapper  z-depth-1-half" v-show="showNodeLabelPanel">
        <div class="modal-header">
            <h5 class="modal-title">Node label settings &nbsp;<i style="font-size:35px;" class="pe page-header-icon pe-7s-paint-bucket"></i></h5>
            <div class="sub-title"><p v-if="allNodeProps.length">Select properties to show on nodes of type "{{nodeType}}".
            </p>
            <p v-else>There is nothing configurable for nodes of type "{{nodeType}}".</p>
          </div>
        </div>
        <div class="panel-body">
            <div class="side-column"></div>
            <div class="properties-list">
                <ul class="dd-list">
                    <li class="dd-item"  @click="configureNode(prop)" v-for="prop in allNodeProps" v-bind:class="{'li-active':currentTypeProperties.includes(prop)}">
                        <div class="dd-handle">{{prop}}</div>
                    </li>
                </ul>
            </div>
            <div class="side-column"></div>
        </div>
        <div class="panel-footer">
            <button class="btn" @click="showNodeLabelPanel=false;">Done</button>
        </div>
    </div>
</transition>
</template>

<style scoped>

.panel-wrapper{
  position: absolute;
  bottom: 100%;
  background-color: #0f0f0f;
  width:250px;
  margin-bottom:5px;
  padding: 10px 30px;
  display: flex;
  flex-direction: column;
  left:15px;
}

.modal-title{
  margin-bottom:20px;
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-weight: 400;
}

.sub-title{
  font-size:85%;
}

.modal-header{
  color: white;
}

.panel-footer{
  display: flex;
  justify-content: flex-end;
}

.panel-body{
  display: flex;
  flex-direction: row;
  margin-bottom: 25px;
}

.side-column{
  display: flex;
  flex: 1;
}

.li-active{
  background-color: #3d404c;
  color: white;
}

.properties-list{
  display: flex;
  flex:3;
  justify-content: center;
  margin-top:20px;
}
.dd-list{
  width: 100%;
}

.dd-item{
  cursor: pointer;
  padding: 5px;
  border: 1px solid #3d404c;
  border-radius: 3px;
  margin: 2px 0px;
  width: 100%;
  font-size: 110%;
}

</style>

<script>

import GraphPageState from '../../../js/state/graphPageState';


export default {
    name: 'NodeLabelPanel',
    data() {
        return {
            state:GraphPageState,
            showNodeLabelPanel:false,
            allNodeProps: [],
            currentTypeProperties: {},
            nodeType: undefined,
            allNodeProps: [],
            selectedProps: [],
        };
    },
    created() {
        this.state.eventHub.$on('show-label-panel',this.openNodeLabelPanel);
    },
    mounted() {
        this.$nextTick(function nextTickVisualiser() {

        });
    },
    methods: {
      configureNode(p) {
          if (this.selectedProps[this.nodeType].includes(p)) {
              this.selectedProps[this.nodeType] = this.selectedProps[this.nodeType].filter(x => x !== p);
          } else {
              this.selectedProps[this.nodeType].push(p);
          }
          this.currentTypeProperties = this.selectedProps[this.nodeType];
          this.state.eventHub.$emit('configure-node',this.nodeType, this.selectedProps[this.nodeType]);
      },
      openNodeLabelPanel(allNodePropsParam,nodeTypeParam){
        this.allNodeProps=allNodePropsParam;
        this.nodeType=nodeTypeParam;
        this.currentTypeProperties = this.selectedProps[this.nodeType];

        if (this.currentTypeProperties === undefined) {
            this.currentTypeProperties = [];
            this.selectedProps[this.nodeType] = [];
        }
        this.showNodeLabelPanel=true;
      },
    }
};
</script>
