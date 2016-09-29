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
                <div class="panel-heading">
                    <div class="panel-tools">
                        <i @click="clearGraph" class="pe-7s-refresh"></i>
                    </div>
                    <h3>Graql Visualiser</h3>
                </div>
                <div class="panel-body">
                    <div class="form-group">
                        <textarea class="form-control" rows="3" placeholder=">>" v-model="graqlQuery" @keydown.enter="notify($event)" @keydown.delete="clearGraph($event)"></textarea>
                    </div>
                    <div class="from-buttons">
                        <button @click="notify" class="btn btn-default search-button">Submit<i class="pe-7s-angle-right-circle"></i></button>
                        <button @click="clearGraph" class="btn btn-default">Clear<i class="pe-7s-refresh"></i></button>
                        <button @click="getMetaTypes" class="btn btn-info">Show Types<i class="types-button" v-bind:class="[typeInstances ? 'pe-7s-angle-up-circle' : 'pe-7s-angle-down-circle']"></i></button>
                    </div>
                </div>
            </div>
            <div class="panel panel-c-info panel-collapse" v-show="typeInstances">
                <div class="panel-body">

                    <div v-for="k in typeKeys">
                        <h4>
                            <button @click="toggleElement(k+'-group')" class="btn btn-link">{{k | capitalize }}</button>
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
            halParser: {},

            typeInstances: false,
            typeKeys: []
        }
    },

    created() {
        visualiser = new Visualiser();
        visualiser.setOnDoubleClick(this.doubleClick)
                  .setOnRightClick(this.rightClick)
                  .setOnClick(this.leftClick);

        engineClient = new EngineClient();

        halParser = new HALParser();
        halParser.setNewResource((id, p) => { visualiser.addNode(id, p.label, p.type, p.baseType, p.ontology) });
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
    },

    methods: {
        typeQuery(t, ti) {
            this.graqlQuery = "match $x "+(t === 'roles' ? 'plays-role':'isa')+" "+ti+";";
            this.typeInstances = false;
            this.notify();
        },

        typeQueryResponse(resp, err) {
            if(resp != undefined)
                halParser.parseHalObject(resp);
            else
                this.showError(err);
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
                if(!halParser.parseResponse(resp))
                    this.showWarning("Sorry, no results found for your query.");
            } else {
                this.showError(err);
            }
        },

        shellResponse(resp, err) {
            if(resp != null)
                this.graqlResponse = Prism.highlight(resp, PLang.graql);
            else
                this.showError(err);
        },

        notify(ev) {
            // Shift + Enter just adds a new line.
            if(ev instanceof KeyboardEvent && ev.shiftKey)
                return;

            if(this.graqlQuery == undefined)
                return;

            // Enable graph animation.
            visualiser.setSimulation(true);

            engineClient.graqlHAL(this.graqlQuery, this.graphResponse);
            engineClient.graqlShell(this.graqlQuery, this.shellResponse);
            this.resetMsg();

            // Dont insert newline.
            ev.preventDefault();
        },

        clearGraph(ev) {
            if(ev instanceof KeyboardEvent && !ev.shiftKey)
                return;

            // Reset all interface elements to default.
            this.graqlQuery = undefined;
            this.graqlResponse = undefined;
            this.resetMsg();

            // And clear the graph
            visualiser.clearGraph();
        },

        leftClick(param) {
            const eventKeys = param.event.srcEvent;
            if(!eventKeys.altKey)
                return;

            _.map(param.nodes, x => { engineClient.request({
                                        url: visualiser.nodes._data[x].ontology,
                                        callback: this.typeQueryResponse
                                    })
            });

        },

        doubleClick(param) {
            const eventKeys = param.event.srcEvent;
            if(!eventKeys.shiftKey)
                visualiser.clearGraph();

            // Enable graph animation
            visualiser.setSimulation(true);

            _.map(param.nodes, x => { engineClient.request({url: x, callback: this.typeQueryResponse}) });
        },

        rightClick(param) {
            param.nodes.map(x => { visualiser.deleteNode(x) });
        },

        suppressEventDefault(e) {
            e.preventDefault();
        },

        getMetaTypes() {
            if(this.typeInstances)
                this.typeInstances = false;
            else
                engineClient.getMetaTypes(x => { if(x != null){ this.typeInstances = x; this.typeKeys = _.keys(x) } });
        },

        toggleElement(e) {
            $('.'+e).toggle();
        }
    }
}
</script>
