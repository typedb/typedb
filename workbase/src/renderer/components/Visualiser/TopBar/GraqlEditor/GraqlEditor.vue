<!--suppress ALL -->
<template>
    <div class="graqlEditor-container">
        <div class="left">
            <vue-button rightIcon="locate" className="vue-button" v-on:clicked="toggleTypesContainer"></vue-button>
            <vue-button icon="star" className="vue-button" v-on:clicked="toggleFavQueriesList"></vue-button>
        </div>

    <div class="center">
        <div class="center-wrapper" v-bind:style="[!currentKeyspace ? {opacity: 0.5} : {opacity: 1}]">
            <div class="column" v-bind:style="[(editorLinesNumber === 1) ? {'margin-bottom': '10px'} : {'margin-bottom': '0px'}]">
                <div class="row">

                    <div class="editor-tooltip"><vue-tooltip content="type a query" :isOpen="showEditorToolTip" :child="dummyGraqlIcon" className="editor-tooltip" v-on:close-tooltip="showEditorToolTip = false"></vue-tooltip></div>

                    <textarea id="graqlEditor" ref="graqlEditor" rows="3"></textarea>

                    <div v-if="showEditorTab" class="editor-tab">
                        <div @click="clearEditor"><vue-icon icon="cross" iconSize="12" className="tab-icon"></vue-icon></div>


                        <vue-tooltip class="star-tooltip" content="save a query" className="star-tooltip" :isOpen="showStarToolTip" :child="dummyStarIcon" v-on:close-tooltip="showStarToolTip = false"></vue-tooltip>

                        <div @click="toggleAddFavQuery"><vue-icon icon="star" iconSize="11" className="tab-icon"></vue-icon></div>
                        <div v-if="editorLinesNumber > 1 && !editorMinimized" @click="minimizeEditor"><vue-icon icon="double-chevron-up" iconSize="13" className="tab-icon"></vue-icon></div>
                        <div v-else-if="editorLinesNumber > 1 && editorMinimized" @click="maximizeEditor"><vue-icon icon="double-chevron-down" iconSize="13" className="tab-icon"></vue-icon></div>

                    </div>
                </div>
            </div>

            <add-fav-query
                    v-if="showAddFavQuery"
                    :currentQuery="currentQuery"
                    :currentKeyspace="currentKeyspace"
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
                    :localStore="localStore"
                    :currentKeyspace="currentKeyspace"
                    :favQueries="favQueries"
                    v-on:close-fav-queries-panel="toggleFavQueriesList"
                    v-on:refresh-queries="refreshFavQueries">
            </fav-queries-list>
            <types-container
                    v-if="showTypesContainer"
                    :localStore="localStore"
                    v-on:close-types-panel="showTypesContainer = false">
            </types-container>
        </div>
    </div>

<div class="right">
    <vue-button v-on:clicked="runQuery" icon="play" ref="runQueryButton" :loading="loadSpinner" className="vue-button run-btn"></vue-button>
    <vue-button v-on:clicked="clearGraph" icon="refresh" ref="clearButton" className="vue-button"></vue-button>
    <!--<vue-button v-on:clicked="takeScreenshot" icon="camera" className="vue-button"></vue-button>-->
</div>

</div>
</template>

<style scoped>

    .editor-tooltip {
        width: 100%;
        display: flex;
        justify-content: center;
        position: absolute;
        top: 22px;
    }

    .star-tooltip {
        position: absolute;
        top: 10px;
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

  import { Icon } from '@blueprintjs/core';

  import React from 'react';

  import $ from 'jquery';
  import Spinner from '@/components/UIElements/Spinner.vue';
  import { RUN_CURRENT_QUERY, CANVAS_RESET } from '@/components/shared/StoresActions';
  import ImageDataURI from 'image-data-uri';
  import GraqlCodeMirror from './GraqlCodeMirror';
  import FavQueriesSettings from '../FavQueries/FavQueriesSettings';
  import ManagementUtils from '../../VisualiserUtils';
  import FavQueriesList from '../FavQueries/FavQueriesList';
  import TypesContainer from '../TypesContainer';
  import ErrorContainer from '../ErrorContainer';
  import AddFavQuery from '../FavQueries/AddFavQuery';


  export default {
    name: 'GraqlEditor',
    props: ['localStore'],
    components: {
      AddFavQuery,
      ErrorContainer,
      FavQueriesList,
      TypesContainer,
      Spinner,
    },
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
        starIcon: null,
        showStarToolTip: false,
        showAddFavQueryToolTip: false,
        showEditorTab: false,
        dummyGraqlIcon: null,
        dummyStarIcon: null,
        showEditorToolTip: false,
      };
    },
    created() {
      this.renderIcons();
    },
    computed: {
      currentQuery() {
        return this.localStore.getCurrentQuery();
      },
      currentKeyspace() {
        return this.localStore.getCurrentKeyspace();
      },
      loadSpinner() { return this.localStore.showSpinner(); },
    },
    watch: {
      currentQuery(query) {
        if (query.length) this.showEditorTab = true; this.showStarToolTip = false; this.showEditorToolTip = false;


        // We need this check because codeMirror reset the cursor position when calling getValue
        if (query === this.codeMirror.getValue()) return;
        this.codeMirror.setValue(query);
        this.codeMirror.focus();
        // Set the cursor at the end of existing content
        this.codeMirror.setCursor(this.codeMirror.lineCount(), 0);
      },
      currentKeyspace() {
        this.refreshFavQueries();
        if (this.currentKeyspace) {
          this.codeMirror.setOption('readOnly', false);
        }
        this.history.clearHistory();
      },
      favQueries(val) {
        if (!val.length) this.showFavQueriesList = false;
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
          'Shift-Backspace': this.clearGraph,
          'Shift-Ctrl-Backspace': this.clearGraphAndPage,
          'Shift-Up': this.history.undo,
          'Shift-Down': this.history.redo,
        });

        this.initialEditorHeight = $('.CodeMirror').height();

        this.codeMirror.on('change', (codeMirrorObj) => {
          this.localStore.setCurrentQuery(codeMirrorObj.getValue());
          this.editorLinesNumber = codeMirrorObj.lineCount();
        });
      });
    },
    methods: {
      runQuery(event) {
        if (!this.currentKeyspace) this.$emit('keyspace-not-selected');
        else if (!this.currentQuery.length) {
          event.stopPropagation(); // to prevent event propogation to graql editor tooltip
          this.showEditorToolTip = true;
        } else {
          this.showFavQueriesList = false;
          this.showTypesContainer = false;

          this.localStore.setCurrentQuery(
            ManagementUtils.limitQuery(this.currentQuery),
          );

          this.history.addToHistory(this.currentQuery);

          this.localStore.dispatch(RUN_CURRENT_QUERY)
            .catch((err) => {
              this.errorMsg = err.details;
              this.showError = true;
            });
          this.showError = false;
        }
      },
      clearEditor() {
        this.codeMirror.setValue('');
        this.maximizeEditor();
      },
      clearGraph() {
        if (!this.currentKeyspace) this.$emit('keyspace-not-selected');
        else {
          this.localStore.dispatch(CANVAS_RESET);
        }
      },
      toggleAddFavQuery() {
        this.showAddFavQuery = !this.showAddFavQuery;
      },
      toggleFavQueriesList(event) {
        if (!this.currentKeyspace) {
          this.$emit('keyspace-not-selected');
        } else if (!this.favQueries.length) {
          event.stopPropagation(); // to prevent event propogation to fav query tooltip
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
      takeScreenshot() {
        const canvas = document.getElementsByTagName('canvas')[0];

        // save canvas image as data url (png format by default)
        const dataURL = canvas.toDataURL();

        const filePath = '/Users/syedirtazaraza/Desktop/grakn/workbase/screenshots/screenshot';

        ImageDataURI.outputFile(dataURL, filePath);
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
      toggleFavQueryTooltip(val) {
        this.showAddFavQueryToolTip = val;
      },
      renderIcons() {
        this.starIcon = React.createElement(Icon, {
          icon: 'star',
          className: 'tab-icon',
          iconSize: 11,
        });

        // To show graql editor tooltip
        this.dummyGraqlIcon = React.createElement(Icon, {
          icon: 'star',
          className: 'dummy-graql-icon',
        });

        // To show fav query tooltip
        this.dummyStarIcon = React.createElement(Icon, {
          icon: 'star',
          className: 'dummy-star-icon',
        });
      },
    },
  };
</script>

