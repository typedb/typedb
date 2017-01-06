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
<section class="wrapper">
    <side-bar></side-bar>
    <section class="content">
        <div class="container-fluid">
            <graql-editor v-on:click-submit="onClickSubmit" v-on:load-ontology="onLoadOntology" v-on:clear="onClear" v-on:close-error="onCloseError" showVisualise="true" :errorMessage="errorMessage" :errorPanelClass="errorPanelClass"></graql-editor>
            <div class="row" v-show="analyticsStringResponse">
                <div class="col-xs-12">
                    <div class="panel panel-filled analyticsStringPanel">
                        <div class="panel-heading">Analytics Results</div>
                        <div class="panel-body">
                            <pre class="language-graql">{{analyticsStringResponse}}</pre>
                        </div>
                    </div>
                </div>
            </div>
            <div class="row tab-row">
                <div class="tabs-col col-md-12">
                    <div class="tabs-container">
                        <ul class="nav nav-tabs">
                            <li class="active"><a data-toggle="tab" href="#tab-1" aria-expanded="true">Graph</a></li>
                            <li class=""><a data-toggle="tab" href="#tab-3" aria-expanded="false">Help</a></li>
                        </ul>
                        <div class="tab-content">
                            <div id="tab-1" class="tab-pane active">
                                <div class="panel-body graph-panel-body">
                                    <div id="graph-div" ref="graph"></div>
                                    <node-panel :allNodeResources="allNodeResources"
                                    :allNodeOntologyProps="allNodeOntologyProps"
                                    :allNodeLinks="allNodeLinks"
                                    :selectedNodeLabel="selectedNodeLabel"
                                    v-on:close-node-panel="closeNodePanel"
                                    v-on:graph-response="onGraphResponse"></node-panel>
                                </div>
                            </div>
                            <div id="tab-3" class="tab-pane">
                                <div class="panel-body">
                                    <h4>Graql Entry</h4>
                                    <br/>
                                    <div class="table-responsive">
                                        <table class="table table-hover table-striped">
                                            <thead>
                                                <tr>
                                                    <th>Key</th>
                                                    <th>What it does</th>
                                                </tr>
                                            </thead>
                                            <tbody>
                                                <tr>
                                                    <td>ENTER</td>
                                                    <td>Submit Graql query.</td>
                                                </tr>
                                                <tr>
                                                    <td>Shift + Enter</td>
                                                    <td>New line.</td>
                                                </tr>
                                                <tr>
                                                    <td>Shift + Backspace</td>
                                                    <td>Clear graph & current query.</td>
                                                </tr>
                                                <tr>
                                                    <td>Shift + Delete</td>
                                                    <td>Clear graph & current query.</td>
                                                </tr>
                                            </tbody>
                                        </table>
                                    </div>
                                    <br/>
                                    <br/>
                                    <h4>Graph Tab Interaction</h4>
                                    <br/>
                                    <div class="table-responsive">
                                        <table class="table table-hover table-striped">
                                            <thead>
                                                <tr>
                                                    <th>Action</th>
                                                    <th>What it does</th>
                                                </tr>
                                            </thead>
                                            <tbody>
                                                <tr>
                                                    <td>Left Click</td>
                                                    <td>Selects a node or edge.</td>
                                                </tr>
                                                <tr>
                                                    <td>Left Click + Alt</td>
                                                    <td>Show related ontology of selected node(s).</td>
                                                </tr>
                                                <tr>
                                                    <td>Double Click</td>
                                                    <td>Shows instances and isa of selected node(s), <b>WITHOUT</b> clearing the graph of all other non-related nodes.
                                                    </td>
                                                </tr>
                                                <tr>
                                                    <td>Double Click + Shift</td>
                                                    <td>Shows instances and isa of selected node(s), whilst clearing the graph of all other non-related nodes.
                                                    </td>
                                                </tr>
                                                <tr>
                                                    <td>Hold Click</td>
                                                    <td>Show node label configuration menu. You can select what properties to display on the node label.
                                                    </td>
                                                </tr>
                                                <tr>
                                                    <td>Right Click + Shift</td>
                                                    <td>Delete selected node(s).</td>
                                                </tr>
                                                <tr>
                                                    <td>Scroll wheel</td>
                                                    <td>Zoom in/out.</td>
                                                </tr>
                                                <tr>
                                                    <td>Click & Drag</td>
                                                    <td>Move graph.</td>
                                                </tr>
                                            </tbody>
                                        </table>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            <!-- MODAL -->
            <div class="modal fade" id="nodeLabelModal" tabindex="-1" role="dialog" aria-hidden="true" style="display: none;">
                <div class="modal-dialog modal-sm">
                    <div class="modal-content">
                        <div class="modal-header text-center">
                            <h5 class="modal-title">Node settings &nbsp;<i style="font-size:35px;" class="pe page-header-icon pe-7s-paint-bucket"></i></h5>
                        </div>
                        <div class="modal-body">
                            <div class="properties-list">
                                <p v-if="allNodeProps.length">Select properties to show on nodes of type "{{nodeType}}".
                                </p>
                                <p v-else>There is nothing configurable for nodes of type "{{nodeType}}".</p>
                                <br/>
                                <ul class="dd-list">
                                    <li class="dd-item" v-for="prop in allNodeProps" v-bind:class="{'li-active':currentTypeProperties.includes(prop)}">
                                        <div class="dd-handle" @click="configureNode(prop)">{{prop}}</div>
                                    </li>
                                </ul>
                            </div>
                        </div>
                        <div class="modal-footer">
                            <button type="button" class="btn btn-default" data-dismiss="modal">Done</button>
                        </div>
                    </div>
                </div>
            </div>
            <keyspaces-modal></keyspaces-modal>
            <signup-modal></signup-modal>
        </div>
    </section>
</section>
</template>

<style>

</style>

<script>
// Modules
import HALParser, {
    URL_REGEX,
} from '../js/HAL/HALParser';
import * as API from '../js/HAL/APITerms';
import EngineClient from '../js/EngineClient';
import Visualiser from '../js/visualiser/Visualiser';

// Sub-components
const GraqlEditor = require('./graqlEditor.vue');
const NodePanel = require('./nodePanel.vue');


export default {
    name: 'GraphPage',
    components: {
        GraqlEditor,
        NodePanel
    },
    data() {
        return {
            errorMessage: undefined,
            errorPanelClass: undefined,
            halParser: {},
            analyticsStringResponse: undefined,
            typeInstances: false,
            typeKeys: [],
            doubleClickTime: 0,
            // resources keys used to change label of a node type
            allNodeProps: [],
            selectedProps: [],
            nodeType: undefined,
            selectedNodeLabel: undefined,
            // resources attached to the selected node
            currentTypeProperties: {},
            codeMirror: {},

            allNodeOntologyProps: {},
            allNodeResources: {},
            allNodeLinks: {},
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
    },

    mounted() {
        this.$nextTick(function nextTickVisualiser() {
            const graph = this.$refs.graph;
            visualiser.render(graph);

            function resizeElements() {
                // set graph div height
                const divHeight = window.innerHeight - graph.offsetTop - $('#graph-div').offset().top - 20;
                $('#graph-div').height(divHeight);
                // set the height of right panel of same size of graph-div
                $('.properties-tab').height(divHeight + 7);
                // fix the height of panel-body so that it is possible to make it overflow:scroll
                $('.properties-tab .panel-body').height(divHeight - 85);
            }
            resizeElements();
            window.onresize = resizeElements;

            $('.properties-tab').hide();
            const height = window.innerHeight - graph.offsetTop - ($('#graph-div').offset().top + 20);
            // make the list of resources tab resizable with mouse - jQueryUI
            $('#list-resources-tab').resizable({
                // make it fixed height and only resizable towards west
                minHeight: height,
                maxHeight: height,
                handles: 'w',
            });
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
            this.errorMessage = undefined;

            if (query.includes('aggregate')) {
                this.showWarning("Invalid query: 'aggregate' queries are not allowed from the Graph page. Please use the Console page.");
                return;
            }

            if (query.trim().startsWith('compute')) {
                EngineClient.graqlAnalytics(query, this.onGraphResponseAnalytics);
            } else {
                EngineClient.graqlHAL(query, this.onGraphResponse);
            }
        },
        /*
         * User interaction: visualiser
         */
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
                    id: props.uuid,
                    type: props.type,
                    baseType: props.baseType,
                };

                this.allNodeResources = this.prepareResources(props.properties);
                this.selectedNodeLabel = visualiser.getNodeLabel(node);

                $('#list-resources-tab').addClass('active');
                this.openPropertiesTab();
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

            if (visualiser.getNode(node).baseType === API.GENERATED_RELATION_TYPE) {
                visualiser.deleteNode(node);
            }

            if (eventKeys.shiftKey) {
                visualiser.clearGraph();
            }

            EngineClient.request({
                url: node,
                callback: this.onGraphResponse,
            });
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
        openPropertiesTab() {
            $('.properties-tab.active').addClass('animated slideInRight');
            $('.properties-tab.active').show();
        },
        /*
         * User interaction: visual elements control
         */
        configureNode(p) {
            if (this.selectedProps[this.nodeType].includes(p)) {
                this.selectedProps[this.nodeType] = this.selectedProps[this.nodeType].filter(x => x !== p);
            } else {
                this.selectedProps[this.nodeType].push(p);
            }
            this.currentTypeProperties = this.selectedProps[this.nodeType];

            visualiser.setDisplayProperties(this.nodeType, this.selectedProps[this.nodeType]);
        },
        closeNodePanel() {
            if ($('.properties-tab.active').hasClass('slideInRight')) {
                $('.properties-tab.active').removeClass('animated slideInRight');
                $('.properties-tab.active').fadeOut(300, () => {
                    this.nodeType = undefined;
                    this.allNodeProps = [];
                    this.selectedProps = [];
                });
                $('.properties-tab.active').removeClass('active');
            }
        },
        holdOnNode(param) {
            const node = param.nodes[0];
            if (node === undefined) return;

            this.allNodeProps = visualiser.getAllNodeProperties(node);
            this.nodeType = visualiser.getNodeType(node);
            this.currentTypeProperties = this.selectedProps[this.nodeType];

            if (this.currentTypeProperties === undefined) {
                this.currentTypeProperties = [];
                this.selectedProps[this.nodeType] = [];
            }
            $('#nodeLabelModal').modal('show');
        },

        onGraphResponseAnalytics(resp, err) {
            if (resp != null) {
                if (resp.type === 'string') {
                    this.analyticsStringResponse = resp.response;
                } else {
                    this.halParser.parseResponse(resp.response);
                    visualiser.fitGraphToWindow();
                }
            } else {
                this.showError(err);
            }
        },

        onCloseError() {
            this.errorMessage = undefined;
        },

        /*
         * EngineClient callbacks
         */
        onGraphResponse(resp, err) {
            if (resp !== undefined) {
                if (!this.halParser.parseResponse(resp)) {
                    this.showWarning('Sorry, no results found for your query.');
                } else {
                    visualiser.cluster();
                }
                visualiser.fitGraphToWindow();
            } else {
                this.showError(err);
            }
        },

        showError(msg) {
            this.errorPanelClass = 'panel-c-danger';
            this.errorMessage = msg;
            $('.search-button').removeClass('btn-default').addClass('btn-danger');
        },

        showWarning(msg) {
            this.errorPanelClass = 'panel-c-warning';
            this.errorMessage = msg;
            $('.search-button').removeClass('btn-default').addClass('btn-warning');
        },

        resetMsg() {
            this.errorMessage = undefined;
            this.analyticsStringResponse = undefined;
            $('.search-button')
                .removeClass('btn-danger')
                .removeClass('btn-warning')
                .addClass('btn-default');
        },

        onClear() {
            // Reset all interface elements to default.
            this.closeNodePanel();
            this.resetMsg();

            // And clear the graph
            visualiser.clearGraph();
        },
    },
};
</script>
