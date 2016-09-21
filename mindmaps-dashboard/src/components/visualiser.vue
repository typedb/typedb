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
            <div class="panel panel-filled">
                <div class="panel-heading">
                    <div class="panel-tools">
                        <i @click="clearGraph" class="pe-7s-refresh"></i>
                    </div>
                    <h3>Graql Visualiser</h3>
                </div>
                <div class="panel-body">
                    <div class="form-group">
                        <textarea class="form-control" rows="3" placeholder=">>" v-model="graqlQuery"></textarea>
                    </div>
                    <div class="from-buttons">
                        <button @click="notify" class="btn btn-default search-button">Submit<i class="pe-7s-angle-right-circle"></i></button>
                        <button @click="clearGraph" class="btn btn-default">Clear<i class="pe-7s-refresh"></i></button>
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
        <div class="col-xs-12">
            <div class="tabs-container">
                <ul class="nav nav-tabs">
                    <li class="active"><a data-toggle="tab" href="#tab-1" aria-expanded="true">Visualiser</a></li>
                    <li class=""><a data-toggle="tab" href="#tab-2" aria-expanded="false">Console</a></li>
                </ul>
                <div class="tab-content">
                    <div id="tab-1" class="tab-pane active">
                        <div class="panel-body">
                            <div class="graph-div" v-el:graph @contextmenu="suppressEventDefault"></div>
                        </div>
                    </div>
                    <div id="tab-2" class="tab-pane">
                        <div class="panel-body">
                            <pre class="language-graql">{{{graqlResponse}}}</pre>
                        </div>
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
.graph-div {
    height: 60vh;
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
</style>

<script>
import _ from 'underscore';
import Prism from 'prismjs';

import Visualiser from '../js/visualiser/Visualiser.js';
import HALParser from '../js/HAL/HALParser.js';
import EngineClient from '../js/EngineClient.js';
import * as PLang from '../js/prismGraql.js';

export default {
    data() {
        return {
            graqlQuery: undefined,
            errorMessage: undefined,
            errorPanelClass: undefined,
            graqlResponse: undefined,
            visualiser: {},
            engineClient: {},
            halParser: {}
        }
    },

    created() {
        visualiser = new Visualiser();
        visualiser.setOnClick(this.leftClick)
                  .setOnRightClick(this.rightClick);

        engineClient = new EngineClient();

        halParser = new HALParser();
        halParser.setNewResource((id, p) => { visualiser.addNode(id, p.label, p.type, p.baseType) });
        halParser.setNewRelationship((f, t, l) => { visualiser.addEdge(f, t, l) });
    },

    attached() {
        visualiser.render(this.$els.graph);
    },

    methods: {
        typeQueryResponse(resp, err) {
            if(resp != undefined) {
                halParser.parseHalObject(resp);
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
            $('.search-button')
                .removeClass('btn-danger')
                .removeClass('btn-warning')
                .addClass('btn-default');
        },

        graphResponse(resp, err) {
            if(resp != null) {
                if(!halParser.parseResponse(resp)) {
                    this.showWarning("Sorry, no results found for your query.");
                }
                else {
                    visualiser.centerNodes();
                }
            }
            else {
                this.showError(err);
            }
        },

        shellResponse(resp, err) {
            if(resp != null) {
                this.graqlResponse = Prism.highlight(resp, PLang.graql);
            }
            else {
                this.showError(err);
            }
        },

        notify() {
            if(this.graqlQuery == undefined)
                return;

            engineClient.graqlHAL(this.graqlQuery, this.graphResponse);
            engineClient.graqlShell(this.graqlQuery, this.shellResponse);
            this.resetMsg();
        },

        clearGraph() {
            // Reset all interface elements to default
            this.graqlQuery = undefined;
            this.graqlResponse = undefined;
            this.resetMsg();

            // And clear the graph
            visualiser.clearGraph();
        },

        leftClick(param) {
            const eventKeys = param.event.srcEvent;
            if(eventKeys.shiftKey)
                visualiser.clearGraph();

            _.map(param.nodes, x => { engineClient.request({url: x, callback: this.typeQueryResponse}) });
        },

        rightClick(param) {
            param.nodes.map(x => { visualiser.deleteNode(x) });
        },

        suppressEventDefault(e){
            e.preventDefault();
        }
    }
}
</script>
