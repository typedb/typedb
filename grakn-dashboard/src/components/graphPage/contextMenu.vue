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
<div id="context-menu" class="node-panel z-depth-1-half" v-show="showContextMenu">
    <div class="panel-body">
        <div class="dd-header">Query builders</div>
        <div v-bind:class="[selectedNodes.length<2 ? 'dd-item' : 'dd-item active']" @click="emitCommonConcepts">
            <span class="list-key">Common concepts</span>
        </div>
        <div v-bind:class="[selectedNodes.length!==2 ? 'dd-item' : 'dd-item active']" @click="emitShortestPath">
            <span class="list-key">Shortest path</span>
        </div>
        <div v-bind:class="[selectedNodes.length!==2 ? 'dd-item' : 'dd-item active']" @click="emitExploreRelations">
            <span class="list-key">Explore relations</span>
        </div>
        <div class="dd-header">Align nodes</div>
        <div class="dd-item active" @click="alignHorizontally">
            <span class="list-key">Horizontally</span>
        </div>
        <div class="dd-item active" @click="alignVertically">
            <span class="list-key">Vertically</span>
        </div>
        <div class="dd-header" v-show="singleNodeType==='ENTITY'">Relations</div>
        <div class="filters">
            <div v-bind:class="[selectedNodes.length===1 ? 'dd-item active' : 'dd-item']" v-for="(type, key) in filterTypes">
                <span class="list-key" @click="fetchFilteredRelations(type.href)">{{type.value}}</span>
            </div>
        </div>
    </div>
</div>
</template>

<style scoped>
.node-panel {
    position: absolute;
    left: 50%;
    top: 30%;
    background-color: #0f0f0f;
    padding: 7px;
    width: 250px;
    -moz-user-select: none;
    -ms-overflow-style: none;
    overflow: -moz-scrollbars-none;
}

.filters{
    max-height: 190px;
    overflow: scroll;
    -moz-user-select: none;
    -ms-overflow-style: none;
    overflow: -moz-scrollbars-none;
}

ul{
    position:absolute;
    left:80%;
    z-index: 2;
}

.node-panel::-webkit-scrollbar {
    display: none;
}

.dd-header {
    border-bottom: 1px solid #606060;
    padding-bottom: 3px;
    margin: 4px 0px;
    font-size: 105%;
    font-weight: bold;
}

.dd-item {
    display: flex;
    align-items: center;
    opacity: 0.5;
    font-size: 98%;
    margin-top: 5px;
}

.dd-item.active:hover {
    color: #00eca2;
}

.dd-item.active {
    opacity: 0.9;
    cursor: pointer;
}

.list-key {
    display: inline-flex;
    flex: 1;
}
</style>

<script>
import QueryBuilder from './modules/QueryBuilder';
import EngineClient from '../../js/EngineClient';
import * as API from '../../js/util/HALTerms';

export default {
  name: 'ContextMenu',
  props: ['showContextMenu', 'mouseEvent', 'graphOffsetTop'],
  data() {
    return {
      selectedNodes: [],
            // filterTypes only contains relations for now, later change this to object instead of array
      filterTypes: [],
      singleNodeType: undefined,
      nodeOnMousePosition: null,
    };
  },
  watch: {
    showContextMenu(newValue) {
      if (newValue) {
        this.positionContextMenu();
        // Nodes already selected before right clicking
        this.selectedNodes = visualiser.network.getSelectedNodes();
        // Check if the user right clicked on a node.
        this.nodeOnMousePosition = visualiser.network.getNodeAt({ x: this.mouseEvent.clientX, y: (this.mouseEvent.clientY - this.graphOffsetTop) });
        // Set the node type used in the html to decide wether to show the filter section.
        if (this.nodeOnMousePosition !== undefined) this.singleNodeType = visualiser.getNode(this.nodeOnMousePosition).baseType;
        this.checkNodesSelection();
        // If the user right clicked on an Entity node, fetch all the relation filters
        if (this.singleNodeType === API.ENTITY) {
          EngineClient.getConceptTypes(this.nodeOnMousePosition).then(resp => this.populateRelationFiltersList(resp));
        }
      } else {
        this.filterTypes = [];
        this.singleNodeType = undefined;
      }
    },
  },
  created() {},
  mounted() {

  },
  methods: {
    checkNodesSelection() {
        // If only 1 node was selected before right clicking it means the user did not want to user any query builder option
        // so we deselect that node and set as selected the one the user right clicked on.
      if (this.selectedNodes.length < 2 && this.nodeOnMousePosition !== undefined) {
        visualiser.network.unselectAll();
        visualiser.network.setSelection({
          nodes: [this.nodeOnMousePosition],
          edges: [],
        });
        this.selectedNodes = [this.nodeOnMousePosition];
      }
    },
    populateRelationFiltersList(resp) {
      const respObj = JSON.parse(resp).response;
      this.filterTypes = respObj.entities;
    },
    positionContextMenu() {
      const contextMenu = document.getElementById('context-menu');
      contextMenu.style.left = `${this.mouseEvent.clientX}px`;
      contextMenu.style.top = `${this.mouseEvent.clientY - this.graphOffsetTop}px`;
    },
    fetchFilteredRelations(href) {
      this.$emit('fetch-relations', href);
    },
    emitCommonConcepts() {
      if (this.selectedNodes.length >= 2) { this.$emit('type-query', QueryBuilder.commonConceptsBuilder(this.selectedNodes)); }
    },
    emitShortestPath() {
      if (this.selectedNodes.length === 2) { this.$emit('type-query', QueryBuilder.shortestPathBuilder(this.selectedNodes)); }
    },
    emitExploreRelations() {
      if (this.selectedNodes.length === 2) { this.$emit('type-query', QueryBuilder.exploreRelationsBuilder(this.selectedNodes)); }
    },
    alignHorizontally() {
      this.$emit('close-context');
      let previousNode = this.selectedNodes[0];
      visualiser.updateNode({
        id: previousNode,
        x: visualiser.rect.startX,
        y: visualiser.rect.startY,
        fixed: {
          x: true,
          y: true,
        },
      });
      for (let i = 1; i < this.selectedNodes.length; i++) {
        const nodeId = this.selectedNodes[i];
        visualiser.updateNode({
          id: nodeId,
          x: this.computeXHorizontal(nodeId, previousNode),
          y: visualiser.rect.startY,
          fixed: {
            x: true,
            y: true,
          },
        });
        previousNode = nodeId;
      }
    },
    alignVertically() {
      this.$emit('close-context');
      let previousNode = this.selectedNodes[0];
      visualiser.updateNode({
        id: previousNode,
        x: visualiser.rect.startX,
        y: visualiser.rect.startY,
        fixed: {
          x: true,
          y: true,
        },
      });
      for (let i = 1; i < this.selectedNodes.length; i++) {
        const nodeId = this.selectedNodes[i];
        visualiser.updateNode({
          id: nodeId,
          x: visualiser.rect.startX,
          y: this.computeYVertical(nodeId, previousNode),
          fixed: {
            x: true,
            y: true,
          },
        });
        previousNode = nodeId;
      }
    },
    computeXHorizontal(nodeId, previousNode) {
      const nodeSize = this.computeNodeWidth(nodeId);
      const rightBorderX = visualiser.network.getBoundingBox(previousNode).right;
      return rightBorderX + 1 + (nodeSize / 2);
    },
    computeNodeWidth(nodeId) {
      const left = visualiser.network.getBoundingBox(nodeId).left;
      const right = visualiser.network.getBoundingBox(nodeId).right;
      return (left < 0) ? Math.abs(left) + right : right - left;
    },
    computeNodeHeight(nodeId) {
      const top = visualiser.network.getBoundingBox(nodeId).top;
      const bottom = visualiser.network.getBoundingBox(nodeId).bottom;
      return (top < 0) ? Math.abs(top) + bottom : bottom - top;
    },
    computeYVertical(nodeId, previousNode) {
      const nodeSize = this.computeNodeHeight(nodeId);
      const bottomBorderY = visualiser.network.getBoundingBox(previousNode).bottom;
      return bottomBorderY + 1 + (nodeSize / 2);
    },
  },
};
</script>
