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
along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>. -->


<template>
<section class="wrapper">
    <side-bar></side-bar>
    <section class="content">
        <div class="container-fluid">
            <graql-editor v-on:click-submit="onClickSubmit" v-on:clear="onClear" v-on:close-error="onCloseError" :errorMessage="errorMessage" :errorPanelClass="errorPanelClass"></graql-editor>
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
                                    <pre class="language-graql" v-html="graqlResponse"></pre>
                                </div>
                            </div>
                            <div id="tab-3" class="tab-pane">
                                <div class="panel-body">
                                    <h4>Graql Entry</h4>
                                    <br />
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
                                </div>
                            </div>
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
import _ from 'underscore';
import Prism from 'prismjs';
import CodeMirror from 'codemirror';
import placeholder from 'codemirror/addon/display/placeholder.js';
import simpleMode from 'codemirror/addon/mode/simple.js';

import HALParser from '../js/HAL/HALParser.js';
import EngineClient from '../js/EngineClient.js';
import * as PLang from '../js/prismGraql.js';
import simpleGraql from '../js/codemirrorGraql.js';
import User from '../js/User.js'

// Components
var GraqlEditor = require('./graqlEditor.vue')

export default {
    name: "ConsoleView",
    components: {
        GraqlEditor
    },
    data() {
        return {
            errorMessage: undefined,
            errorPanelClass: undefined,
            graqlResponse: undefined,
            halParser: {},
            useReasoner: User.getReasonerStatus(),
            materialiseReasoner:User.getMaterialiseStatus(),
            typeInstances: false,
            typeKeys: [],

            codeMirror: {}
        }
    },

    created() {
    },

    mounted: function() {
        this.$nextTick(function() {
            // code for previous attach() method.
        });
    },

    methods: {
        /*
         * Listener methods on emit from GraqlEditor
         */
        onClickSubmit(query) {
            this.errorMessage = undefined;
            EngineClient.graqlShell(query, this.shellResponse,this.useReasoner,this.materialiseReasoner);
        },
        onClear() {
            this.graqlResponse = undefined;
            this.errorMessage = undefined;
        },
        onCloseError() {
            this.errorMessage = undefined;
        },
        /*
         * EngineClient callbacks
         */
        shellResponse(resp, err) {
            if (resp != null)
                this.graqlResponse = Prism.highlight(resp, PLang.graql);
            else
                this.showError(err);
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
        }

    }
}
</script>
