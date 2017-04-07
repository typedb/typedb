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
<div class="container">
    <div class="inline-flex-1"></div>
    <div class="panel-body z-depth-2">
        <pre class="language-graql" v-html="graqlResponse"></pre>
    </div>
    <div class="inline-flex-1"></div>
</div>
</template>

<style scoped>
.container {
    display: flex;
    flex-direction: row;
    justify-content: center;
    height: 100%;
    width: 100%;
    position: absolute;
}

.inline-flex-1 {
    display: inline-flex;
    flex: 1;
}

.panel-body {
    display: inline-flex;
    flex: 4;
    background-color: green;
    margin-top: 20px;
    margin-bottom: 25px;
    background-color: #0f0f0f;
    margin-left: 15px;
    margin-right: 15px;
    padding: 10px 15px;
    position: relative;
    overflow-y: scroll;
    overflow-x: hidden;
}

.panel-body::-webkit-scrollbar {
    display: none;
}

pre {
    width: 100%;
}
</style>

<script>
import _ from 'underscore';
import Prism from 'prismjs';
import CodeMirror from 'codemirror';
import placeholder from 'codemirror/addon/display/placeholder.js';
import simpleMode from 'codemirror/addon/mode/simple.js';
import * as API from '../js/util/HALTerms';

import HALParser from '../js/HAL/HALParser.js';
import EngineClient from '../js/EngineClient.js';
import PLang from '../js/prismGraql.js';
import User from '../js/User.js'

// Components
import ConsolePageState from '../js/state/consolePageState';


export default {
    name: "ConsolePage",
    data() {
        return {
            graqlResponse: undefined,
            halParser: {},
            useReasoner: User.getReasonerStatus(),
            materialiseReasoner: User.getMaterialiseStatus(),
            typeInstances: false,
            typeKeys: [],
            state: ConsolePageState,
            codeMirror: {}
        }
    },

    created() {
        // Register listened on State events
        this.state.eventHub.$on('click-submit', this.onClickSubmit);
        this.state.eventHub.$on('load-ontology', this.onLoadOntology);
        this.state.eventHub.$on('clear-page', this.onClear);
    },
    beforeDestroy() {
        this.state.eventHub.$off('click-submit', this.onClickSubmit);
        this.state.eventHub.$off('load-ontology', this.onLoadOntology);
        this.state.eventHub.$off('clear-page', this.onClear);
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
            this.queryEngine(query);
        },
        onLoadOntology(type) {
            const querySub = `match $x sub ${type};`;
            EngineClient.graqlShell(querySub).then(this.shellResponse, (err) => {
                this.state.eventHub.$emit('error-message', err.message);
            });
        },
        queryEngine(query){
          EngineClient.graqlShell(query).then(this.shellResponse, (err) => {
              this.state.eventHub.$emit('error-message', err.message);
          });
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
            if (resp.length == 0) {
                this.state.eventHub.$emit('warning-message', 'No results were found for your query.');
            } else {
                this.graqlResponse = Prism.highlight(JSON.parse(resp).response, PLang);
            }
        },

    }
}
</script>
