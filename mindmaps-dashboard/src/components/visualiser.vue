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
                    <div class="form-group from-buttons">
                        <button @click="notify" class="btn btn-default">Submit<i class="pe-7s-angle-right-circle"></i></button>
                        <button @click="clearGraph" class="btn btn-default">Clear<i class="pe-7s-refresh"></i></button>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <div class="row" v-show="errorMessage">
        <div class="col-xs-12">
            <div class="panel panel-filled panel-c-danger">
                <div class="panel-body">
                    {{errorMessage}}
                </div>
            </div>
        </div>
    </div>

    <div class="row graph-row" v-show="graphActive">
        <div class="col-xs-12">
        <div class="panel panel-filled">
            <div class="panel-body">
                <div class="graph-div" v-el:graph @contextmenu="suppressEventDefault"></div>
            </div>
        </div>
        </div>
    </div>

    <div class="row" v-show="graqlResponse">
        <div class="col-xs-12">
            <pre class="language-graql">{{{graqlResponse}}}</pre>
        </div>
    </div>

</div>
</template>

<style>
.search-button {
    width: 100%;
}
.graph-row {
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
            graphActive: false,
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
                this.graphActive = true;
            } else {
                this.showError(err);
            }
        },

        showError(msg) {
            this.errorMessage = msg;
            $('.search-button').removeClass('btn-default').addClass('btn-danger');
        },

        graphResponse(resp, err) {
            if(resp != null) {
                halParser.parseResponse(resp);
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

            this.graphActive = true;
            this.errorMessage = undefined;
            $('.search-button').removeClass('btn-danger').addClass('btn-default');
        },

        clearGraph() {
            this.graqlQuery = undefined;
            this.errorMessage = undefined;
            this.graphActive = false;
            this.graqlResponse = undefined;
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
