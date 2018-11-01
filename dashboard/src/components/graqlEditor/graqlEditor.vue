<!--
GRAKN.AI - THE KNOWLEDGE GRAPH
Copyright (C) 2018 Grakn Labs Ltd

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
-->

<template>
<transition name="slideInDown" appear>
    <div class="graqlEditor-container">
        <div class="left-side">
            <fav-queries-list v-on:type-query="typeFavQuery" ref="savedQueries" :state="state"></fav-queries-list>
            <button @click="toggleTypeInstances" class="btn types-button"><span>Types</span><i style="padding-left:3px;" :class="[showTypeInstances ? 'pe-7s-angle-up-circle' : 'pe-7s-angle-down-circle']"></i>
                      </button>
        </div>
        <div class="center">
            <div class="graqlEditor-wrapper">
                <textarea ref="graqlEditor" class="form-control" rows="3" placeholder=">>"></textarea>
                <div class="types-wrapper">
                    <types-panel :typeInstances="typeInstances" :showTypeInstances="showTypeInstances" v-on:type-query="typeQuery" v-on:load-schema="(type)=>state.eventHub.$emit('load-schema',type)" :state="state"></types-panel>
                </div>
                <message-panel :showMessagePanel="showMessagePanel" :message="message" v-on:close-message="showMessagePanel=false"></message-panel>
            </div>
        </div>
        <div class="right-side">
          <scroll-button :editorLinesNumber="editorLinesNumber" :codeMirror="codeMirror"></scroll-button>
          <add-current-query :current-query="currentQuery" v-on:new-query-saved="refreshSavedQueries" :state="state"></add-current-query>
            <div class="right-buttons">
              <button @click="runQuery" class="btn"><i class="pe-7s-angle-right-circle"></i></button>
              <button @click="clearGraph" @click.shift="clearGraphAndPage" class="btn"><i class="pe-7s-close-circle"></i></button>
              <query-settings :state="state"></query-settings>
              <spinner></spinner>
            </div>
        </div>
    </div>
</transition>
</template>

<style scoped>
.right-buttons{
  position: relative;
  display: inline-flex;
}

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
// Third party libs
import CodeMirror from 'codemirror';
import placeholder from 'codemirror/addon/display/placeholder';
import simpleMode from 'codemirror/addon/mode/simple';
import simpleGraql from '../../js/codemirrorGraql';

// Modules
import EngineClient from '../../js/EngineClient';
import GraphPageState from '../../js/state/graphPageState';
import ConsolePageState from '../../js/state/consolePageState';
import User from '../../js/User';

// Sub-components
import AddCurrentQuery from './addCurrentQuery.vue';
import FavQueriesList from './favQueriesList.vue';
import TypesPanel from './typesPanel.vue';
import MessagePanel from './messagePanel.vue';
import QuerySettings from './querySettings.vue';
import ScrollButton from './scrollButton.vue';
import Spinner from '../Spinner.vue';


export default {
  name: 'GraqlEditor',
  components: {
    AddCurrentQuery,
    FavQueriesList,
    TypesPanel,
    MessagePanel,
    QuerySettings,
    ScrollButton,
    Spinner
  },
  props: ['errorMessage', 'errorPanelClass'],
  data() {
    return {
      graqlResponse: undefined,
      typeInstances: false,
      codeMirror: {},
      currentQuery: undefined,
      state: undefined,
      showTypeInstances: false,
      typesElementId:'2',
      showMessagePanel: false,
      message: undefined,
      editorLinesNumber: 1,
    };
  },
  created() {
    this.loadState();
    this.loadMetaTypeInstances();

        // Global key bindings
    window.addEventListener('keyup', (e) => {
      if (e.keyCode === 13 && !e.shiftKey) this.runQuery();
    });

    this.state.eventHub.$on('show-new-navbar-element',(elementId)=>{if(elementId!=this.typesElementId) this.showTypeInstances=false;});

  },
  mounted() {
    this.$nextTick(() => {
      this.codeMirror = CodeMirror.fromTextArea(this.$refs.graqlEditor, {
        lineNumbers: false,
        theme: 'dracula',
        mode: 'graql',
        viewportMargin: Infinity,
        autofocus: true,
        extraKeys: {
          // Enter key is now binded globally on the window object so that a runQuery can be fired even if the cursor is not in the editor
          // But here we need to bind Enter to a behaviour that is not the default "newLine", otherwise everytime we hit enter the cursor goes to new line.
          Enter: 'goLineEnd',
          'Shift-Enter': 'newlineAndIndent',
          'Shift-Delete': this.clearGraph,
          'Shift-Backspace': this.clearGraph,
          'Shift-Ctrl-Backspace': this.clearGraphAndPage,
        },
      });

      this.codeMirror.on('change', (codeMirrorObj, changeObj) => {
        this.currentQuery = codeMirrorObj.getValue();
        this.editorLinesNumber = codeMirrorObj.lineCount();
      });
    });
  },
  watch: {
        // When the route changes we need to check which state we need to emit the events to.
    $route(newRoute) {
      this.loadState();
    },
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
        this.state.eventHub.$off('keyspace-changed', this.refreshSavedQueries);
      }

      switch (this.$route.fullPath) {
        case '/console':
          this.state = ConsolePageState;
          break;
        case '/graph':
          this.state = GraphPageState;
          break;
      }

            // Register event listeners

      this.state.eventHub.$on('error-message', this.onErrorMessage);
      this.state.eventHub.$on('warning-message', this.onWarningMessage);
      this.state.eventHub.$on('keyspace-changed', this.loadMetaTypeInstances);
      this.state.eventHub.$on('inject-query', this.injectQuery);
      this.state.eventHub.$on('append-query', this.appendQuery);
      this.state.eventHub.$on('keyspace-changed', this.refreshSavedQueries);
    },
    refreshSavedQueries() {
      this.$refs.savedQueries.refreshList();
    },
    loadMetaTypeInstances() {
      EngineClient.getMetaTypes().then((x) => {
        const types = x.filter(x=>!x.implicit);
        this.typeInstances={};
        this.typeInstances.entities = types.filter(x=>x['base-type']==='ENTITY_TYPE').filter(x=>x.label!=='entity').map(x=>x.label).concat().sort();
        this.typeInstances.attributes = types.filter(x=>x['base-type']==='ATTRIBUTE_TYPE').filter(x=>x.label!=='attribute').map(x=>x.label).concat().sort();
        this.typeInstances.relationships = types.filter(x=>x['base-type']==='RELATIONSHIP_TYPE').filter(x=>x.label!=='relationship').map(x=>x.label).concat().sort();

      });
    },
    toggleTypeInstances() {
      this.showTypeInstances = !this.showTypeInstances;
      this.showMessagePanel = false;
      if(this.showTypeInstances){
        this.state.eventHub.$emit('show-new-navbar-element',this.typesElementId);
      }
    },
    onErrorMessage(error) {
      this.showMessagePanel = true;
      this.showTypeInstances = false;
      if(typeof error === "object"){
        this.message = JSON.parse(error.message).exception;
        console.log(error.stack);
      }
      else{
        this.message = JSON.parse(error).exception;
      }
    },
    onWarningMessage(message) {
      toastr.info(message);
    },
    runQuery(ev) {
      let query = this.codeMirror.getValue().trim();
      this.showMessagePanel = false;

            // Empty query.
      if (query === undefined || query.length === 0) { return; }

            // Add trailing semi-colon
      if (query.charAt(query.length - 1) !== ';') {
        query += ';';
        this.codeMirror.setValue(query);
      }

      //If graph page - add limit to avoid overloading the Canvas
      if(this.$route.fullPath==='/graph'){ 
        query = this.limitQuery(query);
        this.codeMirror.setValue(query);
      }

      this.state.eventHub.$emit('click-submit', query);
    },
    limitQuery(query){
        const getRegex = /^((.|\s)*;)\s*(get\b.*;)$/;
        let limitedQuery = query;

        // If there is no `get` the user mistyped the query
        if (getRegex.test(query)) {
          const limitRegex = /.*;\s*(limit\b.*?;).*/;
          const offsetRegex = /.*;\s*(offset\b.*?;).*/;
          const deleteRegex = /^(.*;)\s*(delete\b.*;)$/;
          const match = getRegex.exec(query);
          limitedQuery = match[1];
          const getPattern = match[3];
          if (!(offsetRegex.test(query)) && !(deleteRegex.test(query))) { limitedQuery = `${limitedQuery} offset 0;`; }
          if (!(limitRegex.test(query)) && !(deleteRegex.test(query))) { limitedQuery = `${limitedQuery} limit ${User.getQueryLimit()};`; }
          limitedQuery = `${limitedQuery} ${getPattern}`;
        }
        return limitedQuery;
    },
    updateCurrentQuery() {
      this.currentQuery = this.codeMirror.getValue();
    },
    typeFavQuery(query) {
      this.codeMirror.setValue(query);
    },
    typeQuery(t, ti) {
      this.codeMirror.setValue(`match $x ${t === 'roles' ? 'plays' : 'isa'} ${ti}; get;`);
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
      this.codeMirror.setValue('');
      this.graqlResponse = undefined;
      this.showMessagePanel = false;
      this.message = undefined;
    },
    clearGraphAndPage(ev) {
      this.clearGraph(ev);
            // If shift is pressed while clicking on clear button we will clear also the graph/console.
      this.state.eventHub.$emit('clear-page');
    },
  },
};
</script>
