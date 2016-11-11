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
<div class="container-fluid">
    <div class="row">
        <div class="col-xs-12">
          <div class="panel panel-filled" id="panel-console-container">
              <div class="panel-body row" id="panel-console">
                  <div class="form-group col-xs-8" style="margin-bottom:0px;">
                      <textarea v-el:graql-editor class="form-control" rows="3" placeholder=">>"></textarea>
                  </div>
                  <div class="form-buttons col-xs-4">
                    <button @click="getMetaTypes" class="btn btn-info console-button">Types<i class="types-button"
                                                                                    v-bind:class="[typeInstances ? 'pe-7s-angle-up-circle' : 'pe-7s-angle-down-circle']"></i>
                    </button>
                    <button @click="clearGraph" class="btn btn-default console-button">Clear<i class="pe-7s-refresh"></i>
                    </button>
                      <button @click="runQuery" class="btn btn-default search-button console-button">Submit<i
                              class="pe-7s-angle-right-circle"></i></button>
                    </div>
              </div>
          </div>
        </div>
    </div>

    <div class="row" v-show="typeInstances">
        <div class="col-xs-12">
            <div class="panel panel-filled" style="margin-bottom: 0px; margin-top: 20px;">
                <div class="tabs-col">
                    <div class="tabs-container">
                        <ul class="nav nav-tabs">
                            <li v-for="k in typeKeys"><a data-toggle="tab" href="#{{k}}-tab" aria-expanded="false">{{k | capitalize}}</a></li>
                        </ul>
                    </div>
                    <div class="tab-content">
                        <div v-for="k in typeKeys" id="{{k}}-tab" class="tab-pane">
                            <div class="panel-body types-panel" style="margin: 0px;">
                                <div class="{{k}}-group row m-t-md" style="margin-top: 0px;">
                                    <div class="col-lg-2 col-md-3 col-sm-6 col-xs-6 type-instance" v-for="i in typeInstances[k]">
                                        <button  @click="typeQuery(k, i)" class="btn btn-link">{{i}}</button>
                                    </div>
                                </div>
                            </div>
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
                    <li class="active"><a data-toggle="tab" href="#tab-1" aria-expanded="true">Console</a></li>
                    <li class=""><a data-toggle="tab" href="#tab-3" aria-expanded="false">Help</a></li>
                </ul>
                <div class="tab-content">
                    <div id="tab-1" class="tab-pane active">
                        <div class="panel-body">
                            <pre class="language-graql">{{{graqlResponse}}}</pre>
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
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>

</div>
</template>

<style>

</style>

<script>
import _ from 'underscore';
import Prism from 'prismjs';
import CodeMirror from 'codemirror';
import placeholder from 'codemirror/addon/display/placeholder.js';
import simpleMode from 'codemirror/addon/mode/simple.js';

import HALParser from '../js/HAL/HALParser.js';
import EngineClient from '../js/EngineClient.js';
import * as PLang from '../js/prismGraql.js';
import simpleGraql from '../js/codemirrorGraql.js';

export default {
    data() {
        return {
            errorMessage: undefined,
            errorPanelClass: undefined,
            graqlResponse: undefined,
            engineClient: {},
            halParser: {},

            typeInstances: false,
            typeKeys: [],

            codeMirror: {}
        }
    },

    created() {
        engineClient = new EngineClient();

        halParser = new HALParser();
        halParser.setNewResource((id, p, a) => { visualiser.addNode(id, p, a) });
        halParser.setNewRelationship((f, t, l) => { visualiser.addEdge(f, t, l) });
    },

    attached() {
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
    },

    methods: {
        /*
         * User interaction: queries.
         */
        runQuery(ev) {
            const query = codeMirror.getValue();

            // Empty query.
            if(query == undefined || query.length === 0)
                return;

            engineClient.graqlShell(query, this.shellResponse);
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
         * EngineClient callbacks
         */
        shellResponse(resp, err) {
            if(resp != null)
                this.graqlResponse = Prism.highlight(resp, PLang.graql);
            else
                this.showError(err);
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

        clearGraph(ev) {
            codeMirror.setValue("");

            // Reset all interface elements to default.
            this.graqlResponse = undefined;
            this.resetMsg();
        }
    }
}
</script>
