<!--suppress ALL -->
<template>
    <div class="graqlEditor-container">
        <div class="left">
            <vue-button rightIcon="locate" className="vue-button" :disabled="(currentKeyspace) ? false : true" v-on:clicked="toggleTypesContainer"></vue-button>
            <vue-button icon="star" className="vue-button" :disabled="(currentKeyspace) ? false : true" v-on:clicked="toggleFavQueriesList"></vue-button>
        </div>

    <div class="center">
        <div class="center-wrapper" v-bind:style="[!currentKeyspace ? {opacity: 0.5} : {opacity: 1}]">
            <div class="column">
                <div class="row">
                    <textarea id="graqlEditor" ref="graqlEditor" rows="3"></textarea>
                    <div v-if="currentQuery.length" class="editor-tab">
                        <div @click="clearEditor"><vue-icon icon="cross" iconSize="12" className="tab-icon"></vue-icon></div>
                        <div @click="toggleAddFavQuery"><vue-icon icon="star" iconSize="11" className="tab-icon"></vue-icon></div>
                        <div v-if="editorLinesNumber > 1"><vue-icon icon="double-chevron-up" iconSize="13" className="tab-icon"></vue-icon></div>
                    </div>
                </div>
            </div>

            <add-fav-query
                    v-if="showAddFavQuery"
                    :currentQuery="currentQuery"
                    :currentKeyspace="currentKeyspace"
                    v-on:close-add-query-panel="showAddFavQuery = false"
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
                    v-on:close-fav-queries-panel="showFavQueriesList = false"
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
    <vue-button v-on:clicked="runQuery"icon="play" ref="runQueryButton" :disabled="!currentQuery.length || loadSpinner" :loading="loadSpinner" className="vue-button run-btn"></vue-button>
    <vue-button v-on:clicked="clearEditor" icon="refresh" ref="clearButton" :disabled="!currentQuery.length" className="vue-button"></vue-button>
    <!--<vue-button v-on:clicked="takeScreenshot" icon="camera" className="vue-button"></vue-button>-->
</div>

</div>
</template>

<style scoped>

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
        border-bottom: 1px solid #00eca2;
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
        max-height: 57px;
        flex-direction: column;
        display: flex;
        position: relative;
        float: right;
        padding-top: 1px;
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
    import Spinner from '@/components/UIElements/Spinner.vue';
    import { RUN_CURRENT_QUERY } from '@/components/shared/StoresActions';
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
        };
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
        currentQuery() {
          // We need this check because codeMirror reset the cursor position when calling getValue
          if (this.currentQuery === this.codeMirror.getValue()) return;
          this.codeMirror.setValue(this.currentQuery);
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

          this.codeMirror.on('change', (codeMirrorObj) => {
            this.localStore.setCurrentQuery(codeMirrorObj.getValue());
            this.editorLinesNumber = codeMirrorObj.lineCount();
          });
        });
      },
      methods: {
        runQuery() {
          this.showFavQueriesList = false;
          this.showTypesContainer = false;

          this.localStore.setCurrentQuery(
            ManagementUtils.limitQuery(this.currentQuery),
          );

          this.history.addToHistory(this.currentQuery);

          this.localStore.dispatch(RUN_CURRENT_QUERY).catch((err) => {
            this.errorMsg = err.details;
            this.showError = true;
          });
          this.showError = false;
        },
        clearEditor() {
          this.codeMirror.setValue('');
        },
        typeSelected(type) {
          this.codeMirror.setValue(`match $x isa ${type}; get;`);
          this.runQuery();
        },
        toggleAddFavQuery() {
          this.showAddFavQuery = !this.showAddFavQuery;
        },
        toggleFavQueriesList() {
          if (this.showTypesContainer) this.showTypesContainer = false;
          this.showFavQueriesList = !this.showFavQueriesList;
        },
        toggleTypesContainer() {
          if (this.showFavQueriesList) this.showFavQueriesList = false;
          this.showTypesContainer = !this.showTypesContainer;
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

          canvas.style.backgroundColor = '#1a1a1a';

          // save canvas image as data url (png format by default)
          const dataURL = canvas.toDataURL();

          const filePath = '/Users/syedirtazaraza/Desktop/grakn/workbase/screenshots/screenshot';

          ImageDataURI.outputFile(dataURL, filePath);
        },
      },
    };
</script>
