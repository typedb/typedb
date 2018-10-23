<!--suppress ALL -->
<template>
    <div class="graqlEditor-container">
        <div class="left">
            <button @click="toggleFavQueriesList" class="btn fav-queries-container-btn"><vue-icon icon="star" className="vue-icon"></vue-icon></button>
            <button @click="toggleTypesContainer" class="btn types-container-btn"><vue-icon icon="locate" className="vue-icon"></vue-icon></button>
        </div>
    <div class="center">
        <div class="center-wrapper" v-bind:style="[!currentKeyspace ? {opacity: 0.5} : {opacity: 1}]">
            <div class="column" v-bind:style="[(editorLinesNumber === 1) ? {'margin-bottom': '10px'} : {'margin-bottom': '0px'}]">
                <div class="row">
                    <tool-tip class="editor-tooltip" :isOpen="showEditorToolTip" msg="Type a query" arrowPosition="top" v-on:close-tooltip="showEditorToolTip = false"></tool-tip>
                    <textarea id="graqlEditor" ref="graqlEditor" rows="3"></textarea>
                    <div v-if="showEditorTab" class="editor-tab">
                        <div @click="clearEditor" class="clear-editor"><vue-icon icon="cross" iconSize="10" className="tab-icon"></vue-icon></div>
                        <div @click="toggleAddFavQuery"><vue-icon icon="star" iconSize="10" className="tab-icon add-fav-query-btn"></vue-icon></div>
                        <tool-tip class="star-tooltip" :isOpen="showStarToolTip" msg="Save a query" arrowPosition="top" v-on:close-tooltip="showStarToolTip = false"></tool-tip>
                        <div v-if="editorLinesNumber > 1 && !editorMinimized" @click="minimizeEditor"><vue-icon icon="double-chevron-up" iconSize="12" className="tab-icon"></vue-icon></div>
                        <div v-else-if="editorLinesNumber > 1 && editorMinimized" @click="maximizeEditor"><vue-icon icon="double-chevron-down" iconSize="12" className="tab-icon"></vue-icon></div>
                    </div>
                </div>
            </div>
            <add-fav-query
                    v-if="showAddFavQuery"
                    :currentQuery="currentQuery"
                    :currentKeyspace="currentKeyspace"
                    :favQueries="favQueries"
                    :showAddFavQueryToolTip="showAddFavQueryToolTip"
                    v-on:close-add-query-panel="toggleAddFavQuery"
                    v-on:toggle-fav-query-tooltip="toggleFavQueryTooltip"
                    v-on:refresh-queries="refreshFavQueries">
            </add-fav-query>
            <error-container
                    v-if="showError"
                    :errorMsg="errorMsg"
                    v-on:close-error="showError = false">
            </error-container>
            <fav-queries-list
                    v-if="showFavQueriesList"
                    :currentKeyspace="currentKeyspace"
                    :favQueries="favQueries"
                    v-on:close-fav-queries-panel="toggleFavQueriesList"
                    v-on:refresh-queries="refreshFavQueries">
            </fav-queries-list>
            <types-container
                    v-if="showTypesContainer"
                    v-on:close-types-panel="showTypesContainer = false">
            </types-container>
        </div>
    </div>

<div class="right">
    <loading-button v-on:clicked="runQuery" icon="play" ref="runQueryButton" :loading="showSpinner" className="btn run-btn"></loading-button>
    <button @click="clearGraph" class="btn clear-graph-btn"><vue-icon icon="refresh" className="vue-icon"></vue-icon></button>
    <!--<button @click="takeScreenshot" class="btn"><vue-icon icon="camera" className="vue-icon"></vue-icon></button>-->
</div>

</div>
</template>

<style scoped>

    .editor-tooltip {
        top: 42px;
        left: -30px;
    }

    .star-tooltip {
        position: absolute;
        top: 35px;
        width: 90px;
    }

    .save-query {
        margin-left: 10px;
    }

    .query-name-input {
        width: 491px;
    }

    .row {
        display: flex;
        flex-direction: row;
    }

    .column {
        display: flex;
        flex-direction: column;
        width: 100%;
        background-color: #0f0f0f;
        border: var(--container-darkest-border);
        border-bottom: 1px solid var(--green-4);
    }

    .add-fav-query {
        display: flex;
        flex-direction: row;
        position: relative;
        align-items: center;
        background-color: var(--gray-1);
    }

    .editor-tab {
        align-items: center;
        width: 13px;
        flex-direction: column;
        display: flex;
        position: relative;
        float: right;
        border-left: var(--container-light-border)
    }

    .center-wrapper {
        z-index: 3;
        display: flex;
        flex-direction: column;
        flex: 1;
        border-radius: 3px;
        position: absolute;
        width: 100%;
        top: -15px;
    }

    .left {
        display: inline-flex;
        position: relative;
    }

    .center {
        display: inline-flex;
        flex: 1;
        position: relative;
        flex-direction: column;
        margin: 1px;
        margin-right: var(--element-margin);
        margin-left: var(--element-margin);

    }

    .right {
        display: inline-flex;
    }

    .graqlEditor-container {
        display: flex;
        flex-direction: row;
        flex: 3;
        position: relative;
        align-items: center;
    }
</style>

<script>
import { createNamespacedHelpers } from 'vuex';

import $ from 'jquery';
import Spinner from '@/components/UIElements/Spinner.vue';
import { RUN_CURRENT_QUERY, CANVAS_RESET } from '@/components/shared/StoresActions';
// import ImageDataURI from 'image-data-uri';
import GraqlCodeMirror from './GraqlCodeMirror';
import FavQueriesSettings from '../FavQueries/FavQueriesSettings';
import { limitQuery } from '../../VisualiserUtils';
import FavQueriesList from '../FavQueries/FavQueriesList';
import TypesContainer from '../TypesContainer';
import ErrorContainer from '../ErrorContainer';
import AddFavQuery from '../FavQueries/AddFavQuery';
import ToolTip from '../../../UIElements/ToolTip';


export default {
  name: 'GraqlEditor',
  components: {
    ToolTip,
    AddFavQuery,
    ErrorContainer,
    FavQueriesList,
    TypesContainer,
    Spinner,
  },
  props: ['tabId'],
  data() {
    return {
      codeMirror: {},
      editorLinesNumber: 1,
      showAddFavQuery: false,
      showFavQueriesList: false,
      showTypesContainer: false,
      favQueries: [],
      showError: false,
      errorMsg: '',
      initialEditorHeight: undefined,
      editorMinimized: false,
      showStarToolTip: false,
      showAddFavQueryToolTip: false,
      showEditorTab: false,
      showEditorToolTip: false,
    };
  },
  beforeCreate() {
    const { mapGetters } = createNamespacedHelpers(`tab-${this.$options.propsData.tabId}`);

    // computed
    this.$options.computed = {
      ...(this.$options.computed || {}),
      ...mapGetters(['currentKeyspace', 'currentQuery', 'showSpinner']),
    };
  },
  watch: {
    currentQuery(query) {
      if (query.length) { this.showEditorTab = true; this.showStarToolTip = false; this.showEditorToolTip = false; } else this.showEditorTab = false;

      // We need this check because codeMirror reset the cursor position when calling getValue
      if (query === this.codeMirror.getValue()) return;
      this.codeMirror.setValue(query);
      this.codeMirror.focus();
      // Set the cursor at the end of existing content
      this.codeMirror.setCursor(this.codeMirror.lineCount(), 0);
    },
    currentKeyspace(keyspace) {
      this.refreshFavQueries();
      if (keyspace) {
        this.codeMirror.setOption('readOnly', false);
      }
      this.history.clearHistory();
    },
    favQueries(val) {
      if (!val.length) this.showFavQueriesList = false;
    },
    showAddFavQuery() {
      this.showStarToolTip = false;
    },
  },
  mounted() {
    this.$nextTick(() => {
      this.codeMirror = GraqlCodeMirror.getCodeMirror(this.$refs.graqlEditor);
      this.codeMirror.setOption('readOnly', 'nocursor');
      this.history = GraqlCodeMirror.createGraqlEditorHistory(this.codeMirror);
      this.codeMirror.setOption('extraKeys', {
        Enter: this.runQuery,
        'Shift-Enter': 'newlineAndIndent',
        'Shift-Up': this.history.undo,
        'Shift-Down': this.history.redo,
      });

      this.initialEditorHeight = $('.CodeMirror').height();

      this.codeMirror.on('change', (codeMirrorObj) => {
        this.$store.commit('currentQuery', codeMirrorObj.getValue());
        this.editorLinesNumber = codeMirrorObj.lineCount();
      });
      this.codeMirror.on('focus', () => {
        if (this.editorMinimized) this.maximizeEditor();
      });
    });
  },
  methods: {
    runQuery(event) {
      if (!this.currentKeyspace) this.$emit('keyspace-not-selected');
      else if (!this.currentQuery.length) {
        if (event.stopPropagation) event.stopPropagation(); // to prevent event propogation to graql editor tooltip
        this.showEditorToolTip = true;
      } else {
        this.showFavQueriesList = false;
        this.showTypesContainer = false;

        this.$store.commit(`tab-${this.$options.propsData.tabId}/currentQuery`, limitQuery(this.currentQuery));

        this.history.addToHistory(this.currentQuery);

        this.$store.dispatch(`tab-${this.$options.propsData.tabId}/${RUN_CURRENT_QUERY}`)
          .catch((err) => {
            if (!err.details) this.errorMsg = err.message;
            else this.errorMsg = err.details;
            this.showError = true;
          });
        this.showError = false;
      }
    },
    clearEditor() {
      this.codeMirror.setValue('');
      if (this.editorMinimized) this.maximizeEditor();
    },
    clearGraph() {
      if (!this.currentKeyspace) this.$emit('keyspace-not-selected');
      else {
        this.$store.dispatch(CANVAS_RESET);
      }
    },
    toggleAddFavQuery() {
      this.showAddFavQuery = !this.showAddFavQuery;
    },
    toggleFavQueriesList(event) {
      if (!this.currentKeyspace) {
        this.$emit('keyspace-not-selected');
      } else if (!this.favQueries.length) {
        if (event.stopPropagation) event.stopPropagation(); // to prevent event propogation to fav query tooltip
        this.showEditorTab = true;
        this.showStarToolTip = true;
      } else {
        if (this.showTypesContainer) this.showTypesContainer = false;
        this.showFavQueriesList = !this.showFavQueriesList;
      }
    },
    toggleTypesContainer() {
      if (!this.currentKeyspace) this.$emit('keyspace-not-selected');
      else {
        if (this.showFavQueriesList) this.showFavQueriesList = false;
        this.showTypesContainer = !this.showTypesContainer;
      }
    },
    refreshFavQueries() {
      this.favQueries = this.objectToArray(
        FavQueriesSettings.getFavQueries(this.currentKeyspace),
      );
    },
    objectToArray(object) {
      return Object.keys(object).map(key => ({
        name: key,
        value: object[key].replace('\n', ''),
      }));
    },
    minimizeEditor() {
      $('.CodeMirror').animate({
        height: this.initialEditorHeight,
      }, 300);
      this.editorMinimized = true;
    },
    maximizeEditor() {
      $('.CodeMirror').animate({
        height: $('.CodeMirror-sizer').outerHeight(),
      }, 300, () => {
        $('.CodeMirror').css({
          height: 'auto',
        });
      });
      this.editorMinimized = false;
    },
    // takeScreenshot() {
    //   const canvas = document.getElementsByTagName('canvas')[0];

    //   // save canvas image as data url (png format by default)
    //   const dataURL = canvas.toDataURL();

    //   const filePath = '/screenshots';

    //   this.codeMirror.on('focus', () => {
    //     if (this.editorMinimized) this.maximizeEditor();
    //   });
    //   this.editorMinimized = false;
    // },
    toggleFavQueryTooltip(val) {
      this.showAddFavQueryToolTip = val;
    },
  },
};
</script>

