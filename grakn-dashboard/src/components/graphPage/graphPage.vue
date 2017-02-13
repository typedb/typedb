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
        <div v-on:contextmenu.prevent id="graph-div" ref="graph"></div>
        <node-panel :showNodePanel="showNodePanel" :allNodeResources="allNodeResources" :allNodeOntologyProps="allNodeOntologyProps" :allNodeLinks="allNodeLinks" :selectedNodeLabel="selectedNodeLabel" v-on:graph-response="onGraphResponse" v-on:close-node-panel="showNodePanel=false"></node-panel>
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
import Visualiser from '../../js/visualiser/Visualiser';
import GraphPageState from '../../js/state/graphPageState';

// Sub-components
const NodePanel = require('./nodePanel.vue');
const FooterBar = require('./footer/footerBar.vue');


export default {
    name: 'GraphPage',
    components: {
        NodePanel,
        FooterBar,
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
        };
    },

    created() {
        window.visualiser = new Visualiser();
        visualiser.setOnDoubleClick(this.doubleClick)
            .setOnRightClick(this.rightClick)
            .setOnClick(this.singleClick)
            .setOnDragEnd(this.dragEnd)
            .setOnHoldOnNode(this.holdOnNode);

        this.halParser = new HALParser();

        this.halParser.setNewResource((id, p, a, l) => visualiser.addNode(id, p, a, l));
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
    },
    mounted() {
        this.$nextTick(function nextTickVisualiser() {
            const graph = this.$refs.graph;
            visualiser.render(graph);
        });
    },

    methods: {

        onLoadOntology() {
            const querySub = `match $x sub ${API.ROOT_CONCEPT};`;
            EngineClient.graqlHAL(querySub, this.onGraphResponse);
        },

        singleClick(param) {
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

        onClickSubmit(query) {
            if (query.includes('aggregate')) {
                this.showWarning("Invalid query: 'aggregate' queries are not allowed from the Graph page. Please use the Console page.");
                return;
            }

            if (query.trim().startsWith('compute')) {
                EngineClient.graqlAnalytics(query, this.onGraphResponseAnalytics);
            } else {
                let queryToExecute = query;
                if (!(query.includes('offset')))
                    queryToExecute = queryToExecute + ' offset 0;';
                if (!(query.includes('limit')))
                    queryToExecute = queryToExecute + ' limit 100;';

                this.state.eventHub.$emit('inject-query', queryToExecute);

                EngineClient.graqlHAL(queryToExecute, this.onGraphResponse);
            }
        },
        leftClick(param) {
            // As multiselect is disabled, there will only ever be one node.
            const node = param.nodes[0];
            const eventKeys = param.event.srcEvent;
            const clickType = param.event.type;

            if (node === undefined || eventKeys.shiftKey || clickType !== 'tap') {
                return;
            }

            // When we will enable clustering, also need to check && !visualiser.expandCluster(node)
            if (eventKeys.altKey) {
                if (visualiser.nodes._data[node].ontology) {
                    EngineClient.request({
                        url: visualiser.nodes._data[node].ontology,
                        callback: this.onGraphResponse,
                    });
                }
            } else {
                const props = visualiser.getNode(node);
                this.allNodeOntologyProps = {
                    id: props.id,
                    type: props.type,
                    baseType: props.baseType,
                };

                this.allNodeResources = this.prepareResources(props.properties);
                this.selectedNodeLabel = visualiser.getNodeLabel(node);

                this.showNodePanel = true;
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
        dragEnd(param) {
            // As multiselect is disabled, there will only ever be one node.
            const node = param.nodes[0];
            visualiser.disablePhysicsOnNode(node);
        },

        doubleClick(param) {
            this.doubleClickTime = new Date();
            const node = param.nodes[0];
            if (node === undefined || visualiser.expandCluster(node)) {
                return;
            }

            const eventKeys = param.event.srcEvent;

            if (eventKeys.altKey) {
                if (visualiser.nodes._data[node].ontology) {
                    EngineClient.request({
                        url: visualiser.nodes._data[node].ontology,
                        callback: this.onGraphResponse,
                    });
                }
            } else {
                EngineClient.request({
                    url: visualiser.getNode(node).href,
                    callback: this.onGraphResponse,
                });

                if (visualiser.getNode(node).baseType === API.GENERATED_RELATION_TYPE) {
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
        configureNode(nodeType, selectedProps) {
            visualiser.setDisplayProperties(nodeType, selectedProps);
        },
        holdOnNode(param) {
            const node = param.nodes[0];
            if (node === undefined) return;

            this.state.eventHub.$emit('show-label-panel', visualiser.getAllNodeProperties(node), visualiser.getNodeType(node));
        },

        onGraphResponseAnalytics(resp, err) {
            if (resp != null) {
                if (resp.type === 'string') {
                    this.state.eventHub.$emit('analytics-string-response', resp.response);
                } else {
                    this.halParser.parseResponse(resp.response);
                    visualiser.fitGraphToWindow();
                }
            } else {
                this.state.eventHub.$emit('error-message', err);
            }
        },

        onGraphResponse(resp, err) {
            if (resp != null) {
                if (!this.halParser.parseResponse(resp)) {
                    this.state.eventHub.$emit('warning-message', 'No results were found for your query.');
                } else {
                    visualiser.cluster();
                }
                visualiser.fitGraphToWindow();
            } else {
                this.state.eventHub.$emit('error-message', err);
            }
        },

        onClear() {
            // Reset all interface elements to default.
            this.showNodeLabelPanel = false;
            this.showNodePanel = false;

            // And clear the graph
            visualiser.clearGraph();
        },
    },
};
</script>
