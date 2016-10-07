<!--
MindmapsDB - A Distributed Semantic Database
Copyright (C) 2016  Mindmaps Research Ltd

MindmapsDB is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

MindmapsDB is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
-->

<template>
<div class="container-fluid">
    <div class="row">
        <div class="col-xs-12">
            <div class="panel panel-filled" style="margin-bottom: 0px;">
                <div class="panel-body">
                    <div class="form-group">
                        <textarea v-el:graql-editor class="form-control" rows="3" placeholder=">>"></textarea>
                    </div>
                    <div class="from-buttons">
                        <button @click="runQuery" class="btn btn-default search-button">Submit<i class="pe-7s-angle-right-circle"></i></button>
                        <button @click="clearGraph" class="btn btn-default">Clear<i class="pe-7s-refresh"></i></button>
                        <button @click="getMetaTypes" class="btn btn-info">Show Types<i class="types-button" v-bind:class="[typeInstances ? 'pe-7s-angle-up-circle' : 'pe-7s-angle-down-circle']"></i></button>
                    </div>
                </div>
            </div>
            <div class="panel panel-c-info panel-collapse" v-show="typeInstances">
                <div class="panel-body">
                    <div v-for="k in typeKeys">
                        <h4>
                            <button @click="toggleElement(k+'-group')" class="btn btn-link">{{k | capitalize}}</button>
                        </h4>
                        <div class="row m-t-md type-row btn-group {{k}}-group" style="display: none;">
                            <button v-for="i in typeInstances[k]" @click="typeQuery(k, i)" class="btn btn-default">{{i}}</button>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <div class="row" v-show="errorMessage">
        <div class="col-xs-12">
            <div class="panel panel-filled" v-bind:class="errorPanelClass">
                <div class="panel-body">
                    {{errorMessage}}
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
                        <div class="panel-body">
                            <div class="graph-div" v-el:graph @contextmenu="suppressEventDefault"></div>
                        </div>

                    </div>
                    <div id="tab-3" class="tab-pane">
                        <div class="panel-body">
                            <h4>Graql Entry</h4>
                            <br />
                            <div class="table-responsive">
                                <table class="table table-hover table-striped">
                                    <thead>
                                        <tr><th>Key</th><th>What it does</th></tr>
                                    </thead>
                                    <tbody>
                                        <tr><td>ENTER</td><td>Submit Graql query.</td></tr>
                                        <tr><td>Shift + Enter</td><td>New line.</td></tr>
                                        <tr><td>Shift + Backspace</td><td>Clear graph & current query.</td></tr>
                                        <tr><td>Shift + Delete</td><td>Clear graph & current query.</td></tr>
                                    </tbody>
                                </table>
                            </div>
                            <br />
                            <br />
                            <h4>Graph Tab Interaction</h4>
                            <br />
                            <div class="table-responsive">
                                <table class="table table-hover table-striped">
                                    <thead>
                                        <tr><th>Action</th><th>What it does</th></tr>
                                    </thead>
                                    <tbody>
                                        <tr><td>Left Click</td><td>Selects a node or edge.</td></tr>
                                        <tr><td>Left Click + Alt</td><td>Show related ontology of selected node(s).</td></tr>
                                        <tr><td>Left Click + Shift</td><td>Shows instances and isa of selected node(s), <b>WITHOUT</b> clearing the graph of all other non-related nodes.</td></tr>
                                        <tr><td>Double Click</td><td>Shows instances and isa of selected node(s), whilst clearing the graph of all other non-related nodes.</td></tr>
                                        <tr><td>Right Click</td><td>Show node label configuration menu. You can select what properties to display on the node label.</td></tr>
                                        <tr><td>Right Click + Shift</td><td>Delete selected node(s).</td></tr>
                                        <tr><td>Scroll wheel</td><td>Zoom in/out.</td></tr>
                                        <tr><td>Click & Drag</td><td>Move graph.</td></tr>
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <div class="col-md-2" v-show="nodeType">
            <div class="panel panel-filled panel-c-white">
                <div class="panel-heading">
                    <div class="panel-tools">
                        <a class="panel-close" @click="closeConfigPanel"><i class="fa fa-times"></i></a>
                    </div>
                    Display Configuration
                </div>
                <div class="panel-body">
                    <p v-show="allNodeProps.length">Select properties to be show on nodes of type "{{nodeType}}".</p>
                    <p v-else>Sorry, theres nothing you can configure for nodes of type "{{nodeType}}".</p>
                    <br/>
                    <ul class="dd-list">
                        <li class="dd-item" v-for="prop in allNodeProps" v-bind:class="{'li-active':selectedProps.includes(prop)}">
                            <div class="dd-handle" @click="configureNode(prop)"">{{prop}}</div>
                        </li>
                    </ul>
                </div>
                <div class="panel-footer" style="text-align: right">
                    <button type="button" class="btn btn-warning" @click="closeConfigPanel">Done</button>
                </div>
            </div>
        </div>
    </div>
</div>
</template>

<style>
.tab-row {
    padding-top: 20px;
}
.pe-7s-angle-right-circle {
    padding-left: 5px;
}
.pe-7s-refresh {
    padding-right: 0px;
    padding-left: 5px;
}
.form-buttons {
    padding-bottom: 0px;
    margin-bottom: 0px;
}
.types-button {
    padding-left: 5px;
}
.type-row {
    margin-top: 2px;
    margin-bottom: 10px;
    margin-left: 5px;
}
h4 {
    margin-top: 0px;
    margin-bottom: 0px;
    margin-left: -10px;
}
.li-active {
     background-color: #337ab7;
}
</style>

<script>
import _ from 'underscore';
import Prism from 'prismjs';
import CodeMirror from 'codemirror';
import placeholder from 'codemirror/addon/display/placeholder.js';
import simpleMode from 'codemirror/addon/mode/simple.js';

import Visualiser from '../js/visualiser/Visualiser.js';
import HALParser from '../js/HAL/HALParser.js';
import EngineClient from '../js/EngineClient.js';
import * as PLang from '../js/prismGraql.js';
import simpleGraql from '../js/codemirrorGraql.js';

export default {
    data() {
        return {
            errorMessage: undefined,
            errorPanelClass: undefined,
            visualiser: {},
            engineClient: {},
            halParser: {},

            typeInstances: false,
            typeKeys: [],

            allNodeProps: [],
            selectedProps: [],
            nodeType: undefined,

            codeMirror: {}
        }
    },

    created() {
        visualiser = new Visualiser();
        visualiser.setOnDoubleClick(this.doubleClick)
                  .setOnRightClick(this.rightClick)
                  .setOnClick(this.leftClick);

        engineClient = new EngineClient();

        halParser = new HALParser();
        halParser.setNewResource((id, p, a) => { visualiser.addNode(id, p, a) });
        halParser.setNewRelationship((f, t, l) => { visualiser.addEdge(f, t, l) });
    },

    attached() {
        var graph = this.$els.graph;
        visualiser.render(graph);

        // set window height
        var height = window.innerHeight - graph.offsetTop - $('.graph-div').offset().top;
        $('.graph-div').height(height+"px");

        window.onresize = function() {
            var x = Math.abs(window.innerHeight - graph.offsetTop - $('.graph-div').offset().top);
            $('.graph-div').height(x+"px");
        };

        codeMirror = CodeMirror.fromTextArea(this.$els.graqlEditor, {
                lineNumbers: true,
                theme: "dracula",
                mode: "graql",
                extraKeys: {
                    Enter: this.runQuery,
                    "Shift-Delete": this.clearGraph,
                    "Shift-Backspace": this.clearGraph
                }
            });
        codeMirror.setSize(null, 100);
    },

    methods: {
        /*
         * User interaction: queries.
         */
        runQuery() {
            const query = codeMirror.getValue();

            // Empty query.
            if(query == undefined || query.length === 0)
                return;

            engineClient.graqlHAL(query, this.graphResponse);
            this.resetMsg();
        },

        typeQuery(t, ti) {
            codeMirror.setValue("match $x "+(t === 'roles' ? 'plays-role':'isa')+" "+ti+";");
            this.typeInstances = false;
            this.runQuery();
        },

        getMetaTypes() {
            if(this.typeInstances)
                this.typeInstances = false;
            else
                engineClient.getMetaTypes(x => { if(x != null){ this.typeInstances = x; this.typeKeys = _.keys(x) } });
        },

        /*
         * User interaction: visualiser
         */
        leftClick(param) {
            // As multiselect is disabled, there will only ever be one node.
            const node = param.nodes[0];
            const eventKeys = param.event.srcEvent;

            if(!eventKeys.altKey || node == undefined)
                return;

            if(!visualiser.expandCluster(node))
                engineClient.request({ url: visualiser.nodes._data[x].ontology,
                                       callback: this.typeQueryResponse });
        },

        doubleClick(param) {
            const node = param.nodes[0];
            if(node == undefined || visualiser.expandCluster(node))
                return;

            const eventKeys = param.event.srcEvent;
            if(!eventKeys.shiftKey)
                visualiser.clearGraph();

            engineClient.request({url: node, callback: this.typeQueryResponse});
        },

        rightClick(param) {
            const node = param.nodes[0];
            if(node == undefined)
                return;

            if(param.event.shiftKey) {
                param.nodes.map(x => { visualiser.deleteNode(x) });

            } else if(!visualiser.expandCluster(node)) {
                $('.tabs-col').removeClass('col-md-12').addClass('col-md-10');

                this.allNodeProps = visualiser.getAllNodeProperties(node);
                this.nodeType = visualiser.getNodeType(node);
            }
        },

        /*
         * User interaction: visual elements control
         */
        toggleElement(e) {
            $('.'+e).toggle();
        },

        configureNode(p) {
            if(this.selectedProps.includes(p))
                this.selectedProps = this.selectedProps.filter(x => x != p);
            else
                this.selectedProps.push(p);

            visualiser.setDisplayProperties(this.nodeType, this.selectedProps);
        },

        closeConfigPanel() {
            $('.tabs-col').removeClass('col-md-10').addClass('col-md-12');
            this.nodeType = undefined;
            this.allNodeProps = [];
            this.selectedProps = [];
        },

        /*
         * EngineClient callbacks
         */
        graphResponse(resp, err) {
            if(resp != null) {
                if(!halParser.parseResponse(resp))
                    this.showWarning("Sorry, no results found for your query.");
                else
                    visualiser.cluster();
            } else {
                this.showError(err);
            }
        },

        typeQueryResponse(resp, err) {
            if(resp != undefined) {
                halParser.parseHalObject(resp);
                visualiser.cluster();
            } else {
                this.showError(err);
            }
        },

        /*
         * UX
         */
        suppressEventDefault(e) {
            e.preventDefault();
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
            $('.search-button')
                .removeClass('btn-danger')
                .removeClass('btn-warning')
                .addClass('btn-default');
            this.closeConfigPanel();
        },

        clearGraph() {
            // Reset all interface elements to default.
            codeMirror.setValue("");
            this.resetMsg();

            // And clear the graph
            visualiser.clearGraph();
        }
    }
}
</script>
