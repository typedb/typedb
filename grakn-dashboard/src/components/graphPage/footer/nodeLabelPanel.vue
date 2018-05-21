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
            <h5 class="modal-title"><div class="inner-title"><span class="bold">{{nodeType}}</span><span>label settings</span></div></h5>
            <div class="sub-title">
              <p v-if="!allNodeAttributes.length">There is nothing configurable for nodes of type "{{nodeType}}".</p>
            </div>
        </div>
        <div class="panel-body">
            <div class="side-column"></div>
            <div class="properties-list">
                <ul class="dd-list">                    
                    <li class="dd-item"  @click="configureNode(prop)" v-for="prop in allNodeAttributes" v-bind:class="{'li-active':currentTypeProperties.includes(prop)}">
                        <div class="dd-handle noselect">{{prop}}</div>
                    </li>
                </ul>
            </div>
            <div class="side-column"><colour-picker :node="node"></colour-picker></div>
        </div>
        <div class="panel-footer">
            <button class="btn reset" @click="configureNode()">Reset Label</button>
            <button class="btn reset" @click="resetNodeColour()">Reset Colour</button>
            <button class="btn" @click="showNodeLabelPanel=false;">Done</button>
        </div>
    </div>
</transition>
</template>

<style scoped>

.bold{
  font-weight: bold;
  font-size: 105%;
}

.inner-title{
  display: flex;
  flex-flow: column;
}

.panel-wrapper{
  position: absolute;
  bottom: 100%;
  background-color: #0f0f0f;
  width:300px;
  margin-bottom:5px;
  padding: 10px 30px;
  display: flex;
  flex-direction: column;
  left:15px;
}

.modal-title{
  margin-bottom:20px;
  display: flex;
  justify-content: center;
  align-items: center;
  font-weight: 400;
  margin-top:10px;
}

.sub-title{
  font-size:85%;
}

.modal-header{
  color: white;
}

.panel-footer{
  display: flex;
  justify-content: center;
}

.panel-body{
  display: flex;
  flex-direction: row;
  margin-bottom: 5px;
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
  flex:4;
  justify-content: center;
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

.reset{
  background-color:crimson;
  color: white;
  white-space: nowrap;
}

</style>

<script>

import GraphPageState from '../../../js/state/graphPageState';
import NodeSettings from '../../../js/NodeSettings';

import ColourPicker from './colourPicker.vue';

export default {
  name: 'NodeLabelPanel',
  components: {
    ColourPicker,
  },
  data() {
    return {
      state: GraphPageState,
      showNodeLabelPanel: false,
      allNodeAttributes: [],
      currentTypeProperties: [],
      nodeType: undefined,
      node: undefined,
    };
  },
  created() {
    this.state.eventHub.$on('show-label-panel', this.openNodeLabelPanel);
  },
  mounted() {
    this.$nextTick(() => {

    });
  },
  methods: {
    configureNode(p) {
      if (p == null){   
        const nodeProps = NodeSettings.getLabelProperties(this.nodeType);

        // Reset all node attributes
        for(var i in nodeProps){
          NodeSettings.removeTypeLabel(this.nodeType, nodeProps[i]);;
        }   
      }
      else if (NodeSettings.getLabelProperties(this.nodeType).includes(p)) {
        NodeSettings.removeTypeLabel(this.nodeType, p);
      } else {
        NodeSettings.addTypeLabel(this.nodeType, p);
      }
      this.currentTypeProperties = NodeSettings.getLabelProperties(this.nodeType);
      visualiser.setDisplayProperties(this.nodeType, this.currentTypeProperties);
    },

    // Reset node colour
    resetNodeColour(){
      visualiser.setColourOnNodeType(this.node.baseType, this.node.type);
    },

    openNodeLabelPanel(nodeId) {
      this.node = visualiser.getNode(nodeId);

      // unique values
      this.allNodeAttributes = [...new Set(this.node.attributes.map(x=>x.type))];

      if ((this.node.type !== '')) this.allNodeAttributes.push('type');

      this.allNodeAttributes.sort();

      this.nodeType = this.node.type;
      this.currentTypeProperties = NodeSettings.getLabelProperties(this.nodeType);

      this.showNodeLabelPanel = true;
    },
  },
};
</script>
