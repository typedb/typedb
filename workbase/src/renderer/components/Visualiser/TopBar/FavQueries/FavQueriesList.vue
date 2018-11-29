<template>
        <div class="fav-query-list z-depth-3">
            <div class="column">
                <div class="panel-body" v-if="favQueries.length">
                    <div class="fav-query-item" v-for="(query,index) in favQueries" :key="index">
                        <div class="fav-query-left">
                            <span class="query-name">{{query.name}}</span>
                        </div>
                        <div :class="(isEditable === index) ? 'fav-query-center fav-query-center-editable' : 'fav-query-center'">
                            <input ref="favQuery" :value="index">
                        </div>
                        <div class="fav-query-right">
                            <div class="fav-query-btns">
                                <button v-if="isEditable === index" @click="addFavQuery(index, query.name)" class="btn save-edited-fav-query"><vue-icon icon="floppy-disk" className="vue-icon"></vue-icon></button>
                                <button v-if="isEditable !== index" @click="typeFavQuery(query.value)" class="btn run-fav-query-btn"><vue-icon icon="fast-forward" className="vue-icon"></vue-icon></button>
                                <button v-if="isEditable !== index" @click="editQuery(index)" class="btn edit-fav-query-btn"><vue-icon icon="edit" className="vue-icon"></vue-icon></button>
                                <button v-if="isEditable !== index" @click="removeFavQuery(index, query.name)" class="btn delete-fav-query-btn"><vue-icon icon="trash" className="vue-icon"></vue-icon></button>
                            </div>
                        </div>
                    </div>
                </div>

            </div>
            <div class="editor-tab">
                <div @click="$emit('close-fav-queries-panel')"><vue-icon icon="cross" iconSize="12" className="tab-icon"></vue-icon></div>
            </div>
        </div>
</template>

<style scoped>

    .panel-body {
        padding: 10px;
    }

    .editor-tab {
        max-height: 300px;
        width: 13px;
        flex-direction: column;
        display: flex;
        position: relative;
        float: right;
        border-left: var(--container-light-border);
    }

    .column {
        display: flex;
        flex-direction: column;
        width: 100%;
        overflow-y: auto;
    }

    .column::-webkit-scrollbar {
        width: 2px;
    }

    .column::-webkit-scrollbar-thumb {
        background: var(--green-4);
    }

    .fav-query-left{
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
    }

    /*dynamic*/
    .fav-query-center{
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        width: 100%;
        margin-right: var(--element-margin);
        margin-left: var(--element-margin);
        max-width: 374px;
    }

    .fav-query-right {
        display: flex;
        align-items: center;
        justify-content: center;
    }

    .fav-query-btns{
        display: flex;
        flex-direction: row;
    }

    .query-name {
        width: 100px;
        word-break: break-word;
    }

    .fav-query-item{
        display: flex;
        flex-direction: row;
        padding-top: 5px;
        padding-bottom: 5px;
    }

    .fav-query-item:first-child {
        padding-top: 0px;
    }

    .fav-query-item:last-child {
        border-bottom: none;
        padding-bottom: 0px;
    }


    .fav-query-item:last-child {
        border-bottom: none;
    }

    .fav-query-list {
        background-color: var(--gray-2);
        border: var(--container-darkest-border);
        width: 100%;
        margin-top: 10px;
        max-height: 300px;
        position: relative;
        display: flex;
        flex-direction: row;
    }


</style>

<script>
  import { createNamespacedHelpers } from 'vuex';

  import FavQueriesSettings from './FavQueriesSettings';
  import GraqlCodeMirror from '../GraqlEditor/GraqlCodeMirror';


  export default {
    name: 'FavQueriesList',
    props: ['tabId', 'favQueries'],
    data() {
      return {
        codeMirror: [],
        isEditable: null,
      };
    },
    beforeCreate() {
      const { mapGetters, mapMutations } = createNamespacedHelpers(`tab-${this.$options.propsData.tabId}`);

      // computed
      this.$options.computed = {
        ...(this.$options.computed || {}),
        ...mapGetters(['currentKeyspace']),
      };

      // methods
      this.$options.methods = {
        ...(this.$options.methods || {}),
        ...mapMutations(['setCurrentQuery']),
      };
    },
    mounted() {
      this.$nextTick(() => {
        if (this.favQueries.length) this.renderQueries();
      });
    },
    watch: {
      favQueries() {
        this.$nextTick(() => {
          this.renderQueries();
        });
      },
    },
    methods: {
      typeFavQuery(query) {
        this.setCurrentQuery(query);
      },
      removeFavQuery(index, queryName) {
        FavQueriesSettings.removeFavQuery(queryName, this.currentKeyspace);
        this.$emit('refresh-queries');
        this.codeMirror[index].toTextArea(); // Remove codemirror instance
        this.$notifyInfo(`Query ${queryName} has been deleted from saved queries.`);
      },
      renderQueries() {
        const savedQueries = this.$refs.favQuery;
        savedQueries.forEach((queryInput) => {
          if (queryInput.style.display !== 'none') { // Do not re-render inputs which already have been converted to code mirrors
            const cm = GraqlCodeMirror.getCodeMirror(queryInput);
            cm.setValue(this.favQueries[parseInt(queryInput.value, 10)].value);
            cm.setOption('readOnly', 'nocursor');
            this.codeMirror.push(cm);
          }
        });
      },
      editQuery(index) {
        this.codeMirror[index].setOption('readOnly', false);
        this.codeMirror[index].focus();
        this.isEditable = index;
      },
      addFavQuery(index, queryName) {
        FavQueriesSettings.addFavQuery(
          queryName,
          this.codeMirror[index].getValue(),
          this.currentKeyspace,
        );
        this.$emit('refresh-queries');
        this.isEditable = null;
        this.codeMirror[index].setOption('readOnly', 'nocursor');
        this.$notifyInfo(`Saved query ${queryName} has been updated.`);
      },
    },
  };
</script>
