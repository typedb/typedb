<template>
  <transition name="slideInDown" appear>
    <div class="graqlEditor-container">
      <div class="left-side">            
      </div>
      <div class="center">
        <div class="graqlEditor-wrapper" v-bind:style="[currentKeyspace ? {opacity: 0.5} : {opacity: 1}]">
          <div class="column">
            <div class="row">
              <textarea id="graqlEditor" ref="graqlEditor" class="form-control" rows="3" placeholder=">>"></textarea>
              <div v-if="currentQuery.length" class="editor-tab">
                <img class="tab-btn" @click="clearGraph" src="static/img/icons/icon_close.svg">
                <img class="tab-btn" @click="toggleAddFavQuery" src="static/img/icons/icon_star.svg">
                <img class="tab-btn" v-if="editorLinesNumber > 1" src="static/img/icons/icon_up_arrow.svg">
              </div>
            </div>
            <div v-if="showAddFavQuery" class="add-fav-query">
              <input type="text" class="grakn-input" v-model="currentQueryName" placeholder="query name">
              <img class="btn tab-btn" @click="addFavQuery" :disabled="currentQueryName.length==0" src="static/img/icons/icon_floppy.svg">
            </div>
          </div>
        </div>
      </div>
      <div class="right-side">
        <button id="run-query" 
          @click="runQuery" :class="{'disabled':(currentKeyspace || !currentQuery.length)}" 
          class="btn top-bar-btn" ref="runQueryButton">
          <img src="static/img/icons/icon_play_circle.svg">
        </button>
        <button 
          id="clear"
          :class="{'disabled':(currentKeyspace || !currentQuery.length)}"
          @click="clearGraph" 
          class="btn top-bar-btn" 
          ref="clearButton">
          <img src="static/img/icons/icon_refresh.svg">
        </button>
        <Spinner className="spinner-data" :localStore="localStore"></Spinner>
      </div>
    </div>
  </transition>
</template>

<style scoped>

.row {
  display: flex;
  flex-direction: row;
}

.column {
  display: flex;
  flex-direction: column;
  width: 100%;
}

.add-fav-query{
  display: flex;
  flex-direction: row;
  position: relative;
  justify-content: center;
  align-items: center;
}

.editor-tab {
  align-items: center;
  width: 19px;
  max-height: 57px; 
  flex-direction: column;
  display: flex;
  background-color: var(--medium-color);
  position: relative;
  float: right;
}

.tab-btn {
  height: 20px;
  cursor: pointer;
}
.tab-btn:hover {
  background-color: var(--light-color);
}


span {
    margin-right: 3px;
}

.graqlEditor-wrapper {
    z-index: 3;
    display: flex;
    flex-direction: row;
    flex: 1;
    border-radius: 3px;
    position: absolute;
    width: 100%;
    top: -15px;
    background-color: #0f0f0f;
    border-bottom: 1px solid #00eca2;
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
    flex-direction: column;
}

.right-side {
    display: inline-flex;
}

.disabled{
    opacity:0.5;
    cursor: default;
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
import GraqlCodeMirror from './GraqlCodeMirror';
import AddCurrentQuery from './AddCurrentQuery.vue';
import FavQueriesSettings from '../FavQueries/FavQueriesSettings';
import ManagementUtils from '../../DataManagementUtils';

export default {
  name: 'GraqlEditor',
  props: ['localStore', 'toolTipShown', 'selectedType', 'selectedMetaType'],
  components: {
    AddCurrentQuery,
    Spinner,
  },
  data() {
    return {
      codeMirror: {},
      editorLinesNumber: 1,
      showAddFavQuery: false,
      currentQueryName: '',
    };
  },
  created() {
    this.$on('type-selected', this.typeSelected);
    this.$on('meta-type-selected', this.metaTypeSelected);
  },
  computed: {
    currentQuery() {
      return this.localStore.getCurrentQuery();
    },
    currentKeyspace() {
      return this.localStore.getCurrentKeyspace();
    },
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
      if (this.currentKeyspace) {
        this.codeMirror.setOption('readOnly', false);
      }
      this.history.clearHistory();
    },
  },
  mounted() {
    this.$nextTick(() => {
      this.codeMirror = GraqlCodeMirror.getCodeMirror(this.$refs.graqlEditor);
      // this.codeMirror.setOption('readOnly', 'nocursor');
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

      // this.$refs.addFavQuery.$on('new-fav-query', (currentQueryName) => {
      //   FavQueriesSettings.addFavQuery(currentQueryName, this.currentQuery, this.currentKeyspace);
      //   this.$emit('refresh-fav-queries');
      // });
    });
  },
  methods: {
    runQuery() {
      this.localStore.setCurrentQuery(ManagementUtils.limitQuery(this.currentQuery));

      this.history.addToHistory(this.currentQuery);

      this.toggleToolTip();
      this.localStore.dispatch(RUN_CURRENT_QUERY).catch((err) => { this.$notifyError(err, 'Run Query'); });
    },
    clearGraph() {
      this.codeMirror.setValue('');
    },
    toggleToolTip(val) {
      this.$emit('toggle-tool-tip', val);
    },
    typeSelected(type) {
      this.codeMirror.setValue(`match $x isa ${type}; get;`);
      this.runQuery();
    },
    metaTypeSelected(metaType) {
      this.codeMirror.setValue(`match $x sub ${metaType}; get;`);
      this.runQuery();
    },
    toggleAddFavQuery() {
      this.showAddFavQuery = !this.showAddFavQuery;
    },
    addFavQuery() {
      this.showAddFavQuery = false;
      FavQueriesSettings.addFavQuery(this.currentQueryName, this.currentQuery, this.currentKeyspace);
      this.$emit('refresh-fav-queries');
      this.currentQueryName = '';
    },
  },
};
</script>
