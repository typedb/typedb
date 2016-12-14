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
    <div class="row">
        <div class="col-xs-12">
            <div class="panel panel-filled" id="panel-console-container">
                <div class="panel-body row" id="panel-console">
                    <div class="form-group col-xs-8" style="margin-bottom:0px;">
                        <textarea ref="graqlEditor" class="form-control" rows="3" placeholder=">>"></textarea>
                    </div>
                    <div class="form-buttons col-xs-4">
                      <!-- To be released in 0.9 -->
                        <!-- <button @click="loadFavQueries" class="btn btn-default console-button"><i class="pe-7s-search"></i>
                  </button> -->
                        <button @click="getMetaTypes" class="btn btn-info console-button">Types<i class="types-button" v-bind:class="[typeInstances ? 'pe-7s-angle-up-circle' : 'pe-7s-angle-down-circle']"></i>
                    </button>
                        <button @click="clearGraph" class="btn btn-default console-button">Clear<i class="pe-7s-refresh"></i>
                    </button>
                        <button @click="runQuery" class="btn btn-default search-button console-button">Submit<i
                              class="pe-7s-angle-right-circle"></i></button>
                              <!-- To be released in 0.9 -->
                        <!-- <add-current-query :code-mirror="codeMirror"></add-current-query> -->
                    </div>
                </div>
            </div>
        </div>
    </div>
    <transition name="slide-fade">
        <div v-if="typeInstances" style="margin-bottom: 0px; margin-top: 20px;">
            <div class="panel panel-filled">
                <div class="tabs-col">
                    <div class="row">
                        <div class="col-xs-10">
                            <div class="tabs-container">
                                <ul class="nav nav-tabs">
                                    <li v-for="k in typeKeys"><a data-toggle="tab" v-bind:href="'#'+k+'-tab'" aria-expanded="false">{{k[0].toUpperCase() + k.slice(1)}}</a></li>
                                </ul>
                            </div>
                        </div>
                        <div v-if="showVisualise" class="col-xs-2">
                            <button @click="emitLoadOntology" class="btn btn-default console-button" id="ontology-button">Visualise</button>
                        </div>
                    </div>
                    <div class="tab-content">
                        <div v-for="k in typeKeys" v-bind:id="k+'-tab'" class="tab-pane">
                            <div class="panel-body types-panel" style="margin: 0px;">
                                <div v-bind:class="k+'-group row m-t-md'" style="margin-top: 0px;">
                                    <div class="col-lg-2 col-md-3 col-sm-6 col-xs-6 type-instance" v-for="i in typeInstances[k]">
                                        <button @click="typeQuery(k, i)" class="btn btn-link">{{i}}</button>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </transition>
    <transition name="slide-fade">
        <div v-if="showFavourites" style="margin-bottom: 0px; margin-top: 20px;">
            <div class="panel panel-filled">
                <div class="dd-item" v-for="(value,key) in favouriteQueries">
                    <div @click="typeFavQuery(value)" class="dd-handle"><span class="list-key">{{key}}:</span>
                        <span> {{value}}</span>
                    </div>
                </div>
            </div>
        </div>
    </transition>
    <div class="row" v-show="errorMessage">
        <div class="col-xs-12">
            <div class="panel panel-filled" v-bind:class="errorPanelClass">
                <div class="panel-body">
                    {{errorMessage}} <a href="#" @click="emitCloseError"><i class="pe-7s-close-circle grakn-icon"></i></a>
                </div>
            </div>
        </div>
    </div>
</div>
</template>

<style>
.slide-fade-enter-active {
    transition: all .6s ease;
}

.slide-fade-leave-active {
    transition: all .3s cubic-bezier(1.0, 0.5, 0.8, 1.0);
}

.slide-fade-enter,
.slide-fade-leave-active {
    transform: translateX(10px);
    opacity: 0;
}
</style>

<script>
import _ from 'underscore';
import Prism from 'prismjs';
import CodeMirror from 'codemirror';
import placeholder from 'codemirror/addon/display/placeholder.js';
import simpleMode from 'codemirror/addon/mode/simple.js';

import EngineClient from '../js/EngineClient.js';
import * as PLang from '../js/prismGraql.js';
import simpleGraql from '../js/codemirrorGraql.js';
var addCurrentQuery = require('./addCurrentQuery.vue')
import FavQueries from '../js/FavQueries.js'


export default {
    name: "GraqlEditor",
    components: {
        addCurrentQuery
    },
    props: ['errorMessage', 'errorPanelClass', 'showVisualise'],
    data: function() {
        return {
            graqlResponse: undefined,
            engineClient: {},
            typeInstances: false,
            typeKeys: [],
            codeMirror: {},
            showFavourites: false,
            favouriteQueries: {}
        }
    },
    created: function() {},
    mounted: function() {
        this.$nextTick(function() {
            this.codeMirror = CodeMirror.fromTextArea(this.$refs.graqlEditor, {
                lineNumbers: true,
                theme: "dracula",
                mode: "graql",
                extraKeys: {
                    Enter: this.runQuery,
                    "Shift-Delete": this.clearGraph,
                    "Shift-Backspace": this.clearGraph
                }
            });
        });
    },

    methods: {
        loadFavQueries() {
          if(!this.showFavourites){
            this.favouriteQueries = FavQueries.getFavQueries();
            this.showFavourites = true;
          }else{
            this.showFavourites=false;
          }
        },
        runQuery(ev) {
            const query = this.codeMirror.getValue();

            // Empty query.
            if (query == undefined || query.length === 0)
                return;

            this.$emit('click-submit', query);

            this.resetMsg();
        },
        updateCurrentQuery() {
            this.currentQuery = this.codeMirror.getValue();
        },
        typeFavQuery(query){
          this.codeMirror.setValue(query);
        },
        typeQuery(t, ti) {
            this.codeMirror.setValue("match $x " + (t === 'roles' ? 'plays-role' : 'isa') + " " + ti + ";");
            this.typeInstances = false;
            this.runQuery();
        },

        getMetaTypes() {
            if (this.typeInstances)
                this.typeInstances = false;
            else
                EngineClient.getMetaTypes(x => {
                    if (x != null) {
                        this.typeInstances = x;
                        this.typeKeys = _.keys(x)
                    }
                });
        },
        emitResponseAnalytics(resp, err) {
            this.$emit('response-analytics', resp, err);
        },
        emitCloseError() {
            this.$emit('close-error');
        },

        resetMsg() {
            $('.search-button')
                .removeClass('btn-danger')
                .removeClass('btn-warning')
                .addClass('btn-default');
        },

        emitLoadOntology(ev) {
            this.$emit('load-ontology', ev);
        },

        clearGraph(ev) {
            this.codeMirror.setValue("");
            this.graqlResponse = undefined;
            this.resetMsg();

            this.$emit('clear', ev);
        }
    }
}
</script>
