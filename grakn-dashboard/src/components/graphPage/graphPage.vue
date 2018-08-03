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
<div>
    <div class="graph-panel-body">
        <div v-on:contextmenu="customContextMenu" v-on:mousemove="updateRectangle" id="graph-div" ref="graph"></div>
        <node-panel :showNodePanel="showNodePanel" :node="selectedNodeObject" v-on:load-attribute-owners="onLoadAttributeOwners" v-on:close-node-panel="showNodePanel=false"></node-panel>
        <context-menu :showContextMenu="showContextMenu" :mouseEvent="mouseEvent" :graphOffsetTop="graphOffsetTop" v-on:type-query="emitInjectQuery" v-on:close-context="showContextMenu=false" v-on:fetch-relationships="fetchFilteredRelationships"></context-menu>
        <node-tool-tip :showToolTip="showToolTip" :mouseEvent="mouseEvent" :graphOffsetTop="graphOffsetTop"></node-tool-tip>
        <footer-bar></footer-bar>
    </div>
</div>
</template>

<style scoped>
.graph-panel-body {
    height: 100%;
    width: 100%;
    position: absolute;
}

#graph-div {
    height: 100%;
}
</style>

<script>
/* @flow */

// Modules
import GraphPageState from '../../js/state/graphPageState';
import CanvasHandler from './modules/CanvasHandler';
import Visualiser from '../../js/visualiser/Visualiser';

// Sub-components
import NodePanel from './nodePanel.vue';
import FooterBar from './footer/footerBar.vue';
import ContextMenu from './contextMenu.vue';
import NodeToolTip from './nodeToolTip.vue';

export default {
  name: 'GraphPage',
  components: {
    NodePanel,
    FooterBar,
    ContextMenu,
    NodeToolTip,
  },
  data() {
    return {
      selectedNodeLabel: undefined,
      codeMirror: {},
      selectedNodeObject: undefined,
      showNodePanel: false,
      showContextMenu: false,
      showToolTip: false,
      graphOffsetTop: undefined,
      mouseEvent: undefined,
    };
  },

  created() {
    // Register listened on State events
    GraphPageState.eventHub.$on('clear-page', this.onClear);
    // Events from canvasHandler
    GraphPageState.eventHub.$on('show-node-panel', this.onShowNodePanel);
    GraphPageState.eventHub.$on('hover-node', this.onHoverNode);
    GraphPageState.eventHub.$on('blur-node', this.onBlurNode);
    GraphPageState.eventHub.$on('close-context', () => { this.showContextMenu = false; });
    GraphPageState.eventHub.$on('close-tooltip', () => { this.showToolTip = false; });
  },
  beforeDestroy() {
    // Destroy listeners when component is destroyed - although it never gets destroyed for now. [keep-alive]
    GraphPageState.eventHub.$off('clear-page', this.onClear);
  },
  mounted() {
    this.$nextTick(function nextTickVisualiser() {
      const graph = this.$refs.graph;
      const graphDiv = document.getElementById('graph-div');
      this.graphOffsetTop = graphDiv.getBoundingClientRect().top + document.body.scrollTop;

      window.visualiser = new Visualiser(this.graphOffsetTop);
      CanvasHandler.initialise(graph);
    });
  },

  methods: {
    fetchFilteredRelationships(href) {
      CanvasHandler.fetchFilteredRelationships(href);
      this.showContextMenu = false;
    },
    onShowNodePanel(nodeObject) {
      this.selectedNodeObject = nodeObject;
      this.showNodePanel = true;
    },

    customContextMenu(e) {
      e.preventDefault();
      if (!e.ctrlKey && !e.shiftKey) {
        this.showContextMenu = true;
        this.mouseEvent = e;
      }
    },

    onHoverNode(param) {
      // Mouse event becomes position of hovered node
      this.mouseEvent = param;
      this.showToolTip = true;
    },
    onBlurNode() {
      this.showToolTip = false;
    },

    updateRectangle(e) {
      visualiser.updateRectangle(e.pageX, e.pageY - this.graphOffsetTop);
    },


    configureNode(nodeType, selectedProps) {
      visualiser.setDisplayProperties(nodeType, selectedProps);
    },

    onClear() {
      this.showNodeLabelPanel = false;
      this.showNodePanel = false;
    },

    emitInjectQuery(query) {
      this.showContextMenu = false;
      GraphPageState.eventHub.$emit('inject-query', query);
    },

    onLoadAttributeOwners(attributeId) {
      CanvasHandler.loadAttributeOwners(attributeId);
    },

  },
};
</script>
