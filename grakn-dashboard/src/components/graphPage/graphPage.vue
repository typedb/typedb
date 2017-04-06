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
<div>
    <div class="graph-panel-body">
        <div v-on:contextmenu="customContextMenu" v-on:mousemove="updateRectangle" id="graph-div" ref="graph"></div>
        <node-panel :showNodePanel="showNodePanel" :allNodeResources="allNodeResources" :allNodeOntologyProps="allNodeOntologyProps" :allNodeLinks="allNodeLinks" :selectedNodeLabel="selectedNodeLabel" v-on:graph-response="onGraphResponse" v-on:close-node-panel="showNodePanel=false"></node-panel>
        <context-menu :showContextMenu="showContextMenu" :mouseEvent="mouseEvent" :graphOffsetTop="graphOffsetTop" v-on:type-query="emitInjectQuery" v-on:close-context="showContextMenu=false" v-on:fetch-relations="fetchFilteredRelations"></context-menu>
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
// Modules
import GraphPageState from '../../js/state/graphPageState';
import CanvasHandler from './modules/CanvasHandler';

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
            state: GraphPageState,
            canvasHandler: {},
            selectedNodeLabel: undefined,
            codeMirror: {},
            allNodeOntologyProps: {},
            allNodeResources: {},
            allNodeLinks: {},
            showNodePanel: false,
            showContextMenu: false,
            showToolTip: false,
            graphOffsetTop: undefined,
            mouseEvent: undefined,
        };
    },

    created() {
        this.canvasHandler = new CanvasHandler(this.state);

        // Register listened on State events
        this.state.eventHub.$on('click-submit', query => this.canvasHandler.onClickSubmit(query));
        this.state.eventHub.$on('load-ontology', type => this.canvasHandler.onLoadOntology(type));
        this.state.eventHub.$on('clear-page', this.onClear);
        //Events from canvasHandler
        this.state.eventHub.$on('show-node-panel', this.onShowNodePanel);
        this.state.eventHub.$on('hover-node', this.onHoverNode);
        this.state.eventHub.$on('blur-node', this.onBlurNode);
        this.state.eventHub.$on('close-context', () => this.showContextMenu = false);
        this.state.eventHub.$on('close-tooltip', () => this.showToolTip = false);


    },
    beforeDestroy() {
        // Destroy listeners when component is destroyed - although it never gets destroyed for now. [keep-alive]
        this.state.eventHub.$off('click-submit', this.onClickSubmit);
        this.state.eventHub.$off('load-ontology', this.onLoadOntology);
        this.state.eventHub.$off('clear-page', this.onClear);
    },
    mounted() {
        this.$nextTick(function nextTickVisualiser() {
            const graph = this.$refs.graph;
            const graphDiv = document.getElementById('graph-div');
            this.graphOffsetTop = graphDiv.getBoundingClientRect().top + document.body.scrollTop;
            this.canvasHandler.renderGraph(graph, this.graphOffsetTop);
        });
    },

    methods: {
        fetchFilteredRelations(href){
            this.canvasHandler.fetchFilteredRelations(href);
            this.showContextMenu = false
        },
        onShowNodePanel(ontologyProps, resources, label) {
            this.allNodeOntologyProps = ontologyProps;
            this.allNodeResources = resources;
            this.selectedNodeLabel = label;
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
            // Reset all interface elements to default.
            this.showNodeLabelPanel = false;
            this.showNodePanel = false;

            // And clear the graph
            this.canvasHandler.clearGraph();
        },
        ////// Emits and page elements related methods  ///////


        emitInjectQuery(query) {
            this.showContextMenu = false;
            this.state.eventHub.$emit('inject-query', query);
        },

        onGraphResponse(resp) {
            this.canvasHandler.onGraphResponse(resp);
        }

    },
};
</script>
