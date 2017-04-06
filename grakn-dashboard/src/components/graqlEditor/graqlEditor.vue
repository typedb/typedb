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
<transition name="slideInDown" appear>
    <div class="graqlEditor-container">
        <div class="left-side">
            <fav-queries-list v-on:type-query="typeFavQuery" ref="savedQueries"></fav-queries-list>
            <button @click="toggleTypeInstances" class="btn types-button"><span>Types</span><i style="padding-left:3px;" :class="[showTypeInstances ? 'pe-7s-angle-up-circle' : 'pe-7s-angle-down-circle']"></i>
                      </button>
        </div>
        <div class="center">
            <div class="graqlEditor-wrapper">
                <textarea ref="graqlEditor" class="form-control" rows="3" placeholder=">>"></textarea>
                <div class="types-wrapper">
                    <types-panel :typeInstances="typeInstances" :showTypeInstances="showTypeInstances" v-on:type-query="typeQuery" v-on:load-ontology="(type)=>state.eventHub.$emit('load-ontology',type)"></types-panel>
                </div>
                <message-panel :showMessagePanel="showMessagePanel" :message="message" v-on:close-message="showMessagePanel=false"></message-panel>
            </div>
        </div>
        <div class="right-side">
          <scroll-button :editorLinesNumber="editorLinesNumber" :codeMirror="codeMirror"></scroll-button>
          <add-current-query :current-query="currentQuery" v-on:new-query-saved="refreshSavedQueries"></add-current-query>
            <button @click="runQuery" class="btn"><i
                          class="pe-7s-angle-right-circle"></i></button>
            <button @click="clearGraph" @click.shift="clearGraphAndPage" class="btn"><i class="pe-7s-close-circle"></i>
                          </button>
            <query-settings></query-settings>
        </div>
    </div>
</transition>
</template>

<style scoped>
span {
    margin-right: 3px;
}

.graqlEditor-container {
    display: flex;
    flex-direction: row;
    flex: 3;
    position: relative;
    align-items: center;
}

.graqlEditor-wrapper {
    z-index: 3;
    display: flex;
    flex-direction: column;
    flex: 1;
    border-radius: 3px;
    position: absolute;
    width: 100%;
    top: -15px;
}

.types-wrapper {
    position: relative;
}

.left-side {
    display: inline-flex;
    position: relative;
}

.center {
    display: inline-flex;
    flex: 1;
    position: relative;
}

.right-side {
    display: inline-flex;
}

.grakn-icon {
    margin-left: 10px;
    font-size: 150%;
    margin-top: 15px;
    color: #949ba2;
}
</style>

<script>
//Third party libs
import CodeMirror from 'codemirror';
import placeholder from 'codemirror/addon/display/placeholder.js';
import simpleMode from 'codemirror/addon/mode/simple.js';
import simpleGraql from '../../js/codemirrorGraql.js';

//Modules
import EngineClient from '../../js/EngineClient.js';
import GraphPageState from '../../js/state/graphPageState';
import ConsolePageState from '../../js/state/consolePageState';

//Sub-components
import AddCurrentQuery from './addCurrentQuery.vue';
import FavQueriesList from './favQueriesList.vue';
import TypesPanel from './typesPanel.vue';
import MessagePanel from './messagePanel.vue';
import QuerySettings from './querySettings.vue';
import ScrollButton from './scrollButton.vue';

export default {
    name: "GraqlEditor",
    components: {
        AddCurrentQuery,
        FavQueriesList,
        TypesPanel,
        MessagePanel,
        QuerySettings,
        ScrollButton,
    },
    props: ['errorMessage', 'errorPanelClass'],
    data: function() {
        return {
            graqlResponse: undefined,
            typeInstances: false,
            codeMirror: {},
            currentQuery: undefined,
            state: undefined,
            showTypeInstances: false,
            showMessagePanel: false,
            message: undefined,
        }
    },
    created: function() {
        this.loadState();
        this.loadMetaTypeInstances();

        //Global key bindings
        window.addEventListener('keyup', (e) => {
            if (e.keyCode === 13 && !e.shiftKey) this.runQuery();
        })

    },
    mounted: function() {
        this.$nextTick(function() {
            this.codeMirror = CodeMirror.fromTextArea(this.$refs.graqlEditor, {
                lineNumbers: false,
                theme: "dracula",
                mode: "graql",
                viewportMargin: Infinity,
                autofocus: true,
                extraKeys: {
                    // Enter key is now binded globally on the window object so that a runQuery can be fired even if the cursor is not in the editor
                    // But here we need to bind Enter to a behaviour that is not the default "newLine", otherwise everytime we hit enter the cursor goes to new line.
                    Enter: "goLineEnd",
                    "Shift-Enter": "newlineAndIndent",
                    "Shift-Delete": this.clearGraph,
                    "Shift-Backspace": this.clearGraph,
                    "Shift-Ctrl-Backspace": this.clearGraphAndPage
                }
            });

            this.codeMirror.on("change", (codeMirrorObj, changeObj) => {
                this.currentQuery = codeMirrorObj.getValue();
                this.editorLinesNumber = codeMirrorObj.lineCount();
            });
        });
    },
    watch: {
        // When the route changes we need to check which state we need to emit the events to.
        '$route': function(newRoute) {
            this.loadState();
        }
    },
    methods: {
        loadState() {

            if (this.state) {
                // Destroy listeners on old state when switching to a new one
                this.state.eventHub.$off('error-message', this.onErrorMessage);
                this.state.eventHub.$off('warning-message', this.onWarningMessage);
                this.state.eventHub.$off('keyspace-changed', this.loadMetaTypeInstances);
                this.state.eventHub.$off('inject-query', this.injectQuery);
                this.state.eventHub.$off('append-query', this.appendQuery);
                this.state.eventHub.$off('keyspace-changed',this.refreshSavedQueries);
            }

            switch (this.$route.fullPath) {
                case "/console":
                    this.state = ConsolePageState;
                    break;
                case "/graph":
                    this.state = GraphPageState;
                    break;
            }

            //Register event listeners

            this.state.eventHub.$on('error-message', this.onErrorMessage);
            this.state.eventHub.$on('warning-message', this.onWarningMessage);
            this.state.eventHub.$on('keyspace-changed', this.loadMetaTypeInstances);
            this.state.eventHub.$on('inject-query', this.injectQuery);
            this.state.eventHub.$on('append-query', this.appendQuery);
            this.state.eventHub.$on('keyspace-changed',this.refreshSavedQueries);

        },
        refreshSavedQueries() {
            this.$refs.savedQueries.refreshList();
        },
        loadMetaTypeInstances() {
            EngineClient.getMetaTypes().then(x => {
                if (x != null) {
                    this.typeInstances = JSON.parse(x);
                }
            });
        },
        toggleTypeInstances() {
            this.showTypeInstances = !this.showTypeInstances;
            this.showMessagePanel = false;
        },
        onErrorMessage(message) {
            this.showMessagePanel = true;
            this.showTypeInstances = false;
            this.message = message;
            //set the panel class to be an error class
        },
        onWarningMessage(message) {
            toastr.info(message);
            //set the panel class to be an error class
        },
        runQuery(ev) {
            let query = this.codeMirror.getValue().trim();
            this.showMessagePanel = false;

            // Empty query.
            if (query == undefined || query.length === 0)
                return;

            // Add trailing semi-colon
            if (query.charAt(query.length - 1) !== ';'){
                query += ';';
                this.codeMirror.setValue(query);
            }

            this.state.eventHub.$emit('click-submit', query);
        },
        updateCurrentQuery() {
            this.currentQuery = this.codeMirror.getValue();
        },
        typeFavQuery(query) {
            this.codeMirror.setValue(query);
        },
        typeQuery(t, ti) {
            this.codeMirror.setValue("match $x " + (t === 'roles' ? 'plays' : 'isa') + " " + ti + ";");
            this.showTypeInstances = false;
            this.runQuery();
        },
        injectQuery(query) {
            this.codeMirror.setValue(query);
        },
        appendQuery(query) {
            this.codeMirror.setValue(this.codeMirror.getValue() + query);
        },
        emitResponseAnalytics(resp, err) {
            this.$emit('response-analytics', resp, err);
        },
        emitCloseError() {
            this.$emit('close-error');
        },
        clearGraph(ev) {
            this.codeMirror.setValue("");
            this.graqlResponse = undefined;
            this.showMessagePanel = false;
            this.message = undefined;
        },
        clearGraphAndPage(ev) {
            this.clearGraph(ev);
            // If shift is pressed while clicking on clear button we will clear also the graph/console.
            this.state.eventHub.$emit('clear-page');
        }
    }
}
</script>
