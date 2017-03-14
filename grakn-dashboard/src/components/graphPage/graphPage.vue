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
        <context-menu :showContextMenu="showContextMenu" :mouseEvent="mouseEvent" :graphOffsetTop="graphOffsetTop" v-on:type-query="emitInjectQuery" v-on:close-context="showContextMenu=false"></context-menu>
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
import HALParser, {
    URL_REGEX,
} from '../../js/HAL/HALParser';
import * as API from '../../js/util/HALTerms';
import EngineClient from '../../js/EngineClient';
import User from '../../js/User';
import Visualiser from '../../js/visualiser/Visualiser';
import GraphPageState from '../../js/state/graphPageState';

// Sub-components
const NodePanel = require('./nodePanel.vue');
const FooterBar = require('./footer/footerBar.vue');
const ContextMenu = require('./contextMenu.vue');
const NodeToolTip = require('./nodeToolTip.vue');

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
            halParser: {},
            analyticsStringResponse: undefined,
            doubleClickTime: 0,
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
        window.visualiser = new Visualiser();

        visualiser.setCallbackOnEvent('click', this.singleClick);
        visualiser.setCallbackOnEvent('doubleClick', this.doubleClick);
        visualiser.setCallbackOnEvent('oncontext', this.rightClick);
        visualiser.setCallbackOnEvent('hold', this.holdOnNode);
        visualiser.setCallbackOnEvent('hoverNode', this.hoverNode);
        visualiser.setCallbackOnEvent('blurNode', this.blurNode);
        visualiser.setCallbackOnEvent('dragStart', this.onDragStart);

        this.halParser = new HALParser();

        this.halParser.setNewResource((id, p, a, l, cn) => visualiser.addNode(id, p, a, l, cn));
        this.halParser.setNewRelationship((f, t, l) => visualiser.addEdge(f, t, l));
        this.halParser.setNodeAlreadyInGraph(id => visualiser.nodeExists(id));

        // Register listened on State events
        this.state.eventHub.$on('click-submit', this.onClickSubmit);
        this.state.eventHub.$on('load-ontology', this.onLoadOntology);
        this.state.eventHub.$on('clear-page', this.onClear);
        this.state.eventHub.$on('configure-node', this.configureNode);

    },
    beforeDestroy() {
        // Destroy listeners when component is destroyed - although it never gets detroyed for now. [keep-alive]
        this.state.eventHub.$off('click-submit', this.onClickSubmit);
        this.state.eventHub.$off('load-ontology', this.onLoadOntology);
        this.state.eventHub.$off('clear-page', this.onClear);
        this.state.eventHub.$off('configure-node', this.configureNode);
    },
    mounted() {
        this.$nextTick(function nextTickVisualiser() {
            const graph = this.$refs.graph;

            // TODO: find a way to compute this without jQuery:
            this.graphOffsetTop = $('#graph-div').offset().top;

            visualiser.render(graph);
        });
    },

    methods: {
        updateRectangle(e) {
            visualiser.updateRectangle(e.pageX, e.pageY - this.graphOffsetTop);
        },

        onLoadOntology(type) {
            const querySub = `match $x sub ${type};`;
            EngineClient.graqlHAL(querySub).then(this.onGraphResponse, (err) => {
                this.state.eventHub.$emit('error-message', err.message);
            });
        },
        onClickSubmit(query) {
            if (query.includes('aggregate')) {
                // Error message until we will not properly support aggregate queries in graph page.
                this.state.eventHub.$emit('error-message', 'Invalid query: \'aggregate\' queries are not allowed from the Graph page. Please use the Console page.');
                return;
            }

            if (query.trim().startsWith('compute')) {
                EngineClient.graqlAnalytics(query).then(this.onGraphResponseAnalytics, (err) => {
                    this.state.eventHub.$emit('error-message', err.message);
                });
            } else {
                let queryToExecute = query.trim();

                if (!(query.includes('offset')) && !(query.includes('delete')))
                    queryToExecute = queryToExecute + ' offset 0;';
                if (!(query.includes('limit')) && !(query.includes('delete')))
                    queryToExecute = queryToExecute + ' limit ' + User.getQueryLimit() + ';';
                this.emitInjectQuery(queryToExecute);
                EngineClient.graqlHAL(queryToExecute).then(this.onGraphResponse, (err) => {
                    this.state.eventHub.$emit('error-message', err.message);
                });
            }
        },
        emitInjectQuery(query) {
            this.showContextMenu = false;
            this.state.eventHub.$emit('inject-query', query);
        },
        checkSelectionRectangleStatus(node, eventKeys, param) {
            // If we were drawing rectangle and we click again we stop the drawing and compute selected nodes
            if (visualiser.draggingRect) {
                visualiser.draggingRect = false;
                visualiser.resetRectangle();
                visualiser.network.redraw();
            } else {
                if (eventKeys.ctrlKey && node === undefined) {
                    visualiser.draggingRect = true;
                    visualiser.startRectangle(param.pointer.canvas.x, param.pointer.canvas.y - this.graphOffsetTop);
                }
            }

        },
        /**
         * Prepare the list of resources to be shown in the right div panel
         * It sorts them alphabetically and then check if a resource value is a URL
         */
        prepareResources(originalObject) {
            if (originalObject == null) return {};
            return Object.keys(originalObject).sort().reduce(
                // sortedObject is the accumulator variable, i.e. new object with sorted keys
                // k is the current key
                (sortedObject, k) => {
                    // Add 'href' field to the current object, it will be set to TRUE if it contains a valid URL, FALSE otherwise
                    const currentResourceWithHref = Object.assign({}, originalObject[k], {
                        href: this.validURL(originalObject[k].label)
                    });
                    return Object.assign({}, sortedObject, {
                        [k]: currentResourceWithHref
                    });
                }, {});
        },
        validURL(str) {
            const pattern = new RegExp(URL_REGEX, 'i');
            return pattern.test(str);
        },
        configureNode(nodeType, selectedProps) {
            visualiser.setDisplayProperties(nodeType, selectedProps);
        },
        onGraphResponseAnalytics(resp) {
          const responseObject = JSON.parse(resp);
            if (responseObject.type === 'string') {
                this.state.eventHub.$emit('analytics-string-response', responseObject.response);
            } else {
                this.halParser.parseResponse(responseObject.response, false, false);
                visualiser.fitGraphToWindow();
            }
        },

        onGraphResponse(resp, nodeId) {
            const responseObject = JSON.parse(resp);
            if (!this.halParser.parseResponse(responseObject, false, false, nodeId)) {
                this.state.eventHub.$emit('warning-message', 'No results were found for your query.');
            } else {
                // When a nodeId is provided is because the user double-clicked on a node, so we need to update its href
                // which will contain a new value for offset
                if (nodeId) {
                    visualiser.nodes.update({
                        id: nodeId,
                        href: responseObject._links.self.href,
                    });
                }
            }
            visualiser.fitGraphToWindow();
        },

        onGraphResponseOntology(resp, nodeId) {
            if (!this.halParser.parseResponse(JSON.parse(resp), true, true, nodeId)) {
                this.state.eventHub.$emit('warning-message', 'No results were found for your query.');
            }
            visualiser.fitGraphToWindow();
        },

        onClear() {
            // Reset all interface elements to default.
            this.showNodeLabelPanel = false;
            this.showNodePanel = false;

            // And clear the graph
            visualiser.clearGraph();
        },

        ////////////////////////////////////////////////////// ----------- Graph mouse interactions ------------ ///////////////////////////////////////////////////////////

        holdOnNode(param) {
            const node = param.nodes[0];
            if (node === undefined) return;

            this.state.eventHub.$emit('show-label-panel', visualiser.getAllNodeProperties(node), visualiser.getNodeType(node));
        },

        doubleClick(param) {
            this.doubleClickTime = new Date();
            const node = param.nodes[0];
            if (node === undefined) {
                return;
            }

            const eventKeys = param.event.srcEvent;
            const nodeObj = visualiser.getNode(node);

            if (eventKeys.shiftKey) {
                this.requestOntology(nodeObj);
            } else {
                let generatedNode = false;
                //If we are popping a generated relationship we need to append the 'reasoner' parameter to the URL
                if (nodeObj.baseType === API.GENERATED_RELATION_TYPE) {
                    generatedNode = true;
                }

                EngineClient.request({
                    url: nodeObj.href,
                    appendReasonerParams: generatedNode,
                }).then((resp) => this.onGraphResponse(resp, node), (err) => {
                    this.state.eventHub.$emit('error-message', err.message);
                });
                if (generatedNode) {
                    visualiser.deleteNode(node);
                }
            }
        },
        rightClick(param) {
            const node = param.nodes[0];
            if (node === undefined) {
                return;
            }

            if (param.event.shiftKey) {
                param.nodes.forEach((x) => {
                    visualiser.deleteNode(x);
                });
            }
        },

        hoverNode(param) {
            // Mouse event becomes position of hovered node
            this.mouseEvent = param;
            this.showToolTip = true;
        },
        blurNode() {
            this.showToolTip = false;
        },
        requestOntology(nodeObj) {
            // If alt key is pressed we load ontology related to the current node
            if (nodeObj.ontology) {
                EngineClient.request({
                    url: nodeObj.ontology,
                }).then((resp) => this.onGraphResponseOntology(resp, nodeObj.id), (err) => {
                    this.state.eventHub.$emit('error-message', err.message);
                });
            }
        },
        leftClick(param) {

            // TODO: handle multiselect properly now that is enabled.
            const node = param.nodes[0];
            const eventKeys = param.event.srcEvent;
            const clickType = param.event.type;

            // If it is a long press on node: return and onHold() method will handle the event.
            if (clickType !== 'tap') {
                return;
            }

            // Check if we need to start or stop drawing the selection rectangle
            this.checkSelectionRectangleStatus(node, eventKeys, param);

            if (node === undefined) {
                return;
            }

            const nodeObj = visualiser.getNode(node);

            if (eventKeys.shiftKey) {
                this.requestOntology(nodeObj);
            } else {

                // Show node properties on node panel.
                this.allNodeOntologyProps = {
                    id: nodeObj.id,
                    type: nodeObj.type,
                    baseType: nodeObj.baseType,
                };

                this.allNodeResources = this.prepareResources(nodeObj.properties);
                this.selectedNodeLabel = visualiser.getNodeLabel(node);

                this.showNodePanel = true;
            }

        },

        customContextMenu(e) {
            e.preventDefault();
            if (!e.ctrlKey && !e.shiftKey) {
                this.showContextMenu = true;
                this.mouseEvent = e;
            }
        },

        singleClick(param) {
            // Everytime the user clicks on canvas we clear the context-menu and tooltip
            this.showContextMenu = false;
            this.showToolTip = false;

            const t0 = new Date();
            const threshold = 200;
            // all this fun to be able to distinguish a single click from a double click
            if (t0 - this.doubleClickTime > threshold) {
                setTimeout(() => {
                    if (t0 - this.doubleClickTime > threshold) {
                        this.leftClick(param);
                    }
                }, threshold);
            }
        },

        onDragStart(params) {
            const eventKeys = params.event.srcEvent;
            visualiser.draggingNode = true;
            this.showToolTip = false;
            //If ctrl key is pressed while dragging node/nodes we also unlock and drag the connected nodes
            if (eventKeys.ctrlKey) {
                let neighbours = [];
                params.nodes.forEach(node => {
                    neighbours.push.apply(neighbours, visualiser.network.getConnectedNodes(node));
                    neighbours.push(node);
                });
                visualiser.network.selectNodes(neighbours);
                visualiser.releaseNodes(neighbours);
            } else {
                visualiser.releaseNodes(params.nodes);
            }
        },

        // ----- End of graph interactions ------- //
    },
};
</script>
