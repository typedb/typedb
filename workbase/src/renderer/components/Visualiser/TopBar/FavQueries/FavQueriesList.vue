<template>
    <div class="fav-query-list">
        <div @click="$emit('close-fav-queries-panel')"><vue-icon class="close-container" icon="cross" iconSize="15" className="tab-icon"></vue-icon></div>
        <div class="panel-body" v-if="favQueries.length">
            <div class="fav-query-item" v-for="(query,index) in favQueries" :key="index">
                <div class="fav-query-left">
                    <span class="query-name">{{query.name}}</span>
                    <div class="fav-query-btns">
                        <vue-button v-on:clicked="typeFavQuery(query.value)" text="USE" className="vue-button"></vue-button>
                        <vue-button v-on:clicked="removeFavQuery(index, query.name)" icon="trash" className="vue-button"></vue-button>
                        <!--<div><vue-button icon="edit"></vue-button></div>-->
                    </div>
                </div>
                <div class="fav-query-right">
                    <!--<input type="text" class="grakn-input" v-model="query.value">-->
                    <input ref="favQuery" :value="query.value">
                </div>
            </div>
        </div>
        <div class="panel-body" v-else>
            <div class="dd-item">
                <div class="no-saved">
                    no saved queries
                </div>
            </div>
        </div>
    </div>
</template>

<style scoped>

    .close-container{
        position: absolute;
        right: 0px;
        top:0px;
        height: 15px;
        z-index: 1;
    }

    .fav-query-left{
        padding: var(--container-padding);
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;

    }

    .fav-query-right {
        padding: var(--container-padding);
        width: 100%;
        display: flex;
        align-items: center;
        justify-content: center;
        overflow: auto;
    }

    .fav-query-btns{
        display: flex;
        flex-direction: row;
    }

    .query-name {
        margin-bottom: 5px;
    }

    .fav-query-item{
        border-top: var(--container-light-border);
        display: flex;
        flex-direction: row;
        padding-top: 5px;
    }

    .fav-query-list {
        background-color: var(--gray-2);
        padding: var(--container-padding);
        border: var(--container-darkest-border);
        width: 100%;
        margin-top: 10px;
        max-height: 142px;
        overflow: auto;
        position: relative;
    }
</style>

<script>
  import FavQueriesSettings from '../FavQueries/FavQueriesSettings';
  import GraqlCodeMirror from '../GraqlEditor/GraqlCodeMirror';


  export default {
    name: 'FavQueriesList',
    props: ['localStore', 'currentKeyspace', 'favQueries'],
    data() {
      return {
        codeMirror: {},
      };
    },
    mounted() {
      this.$nextTick(() => {
        this.renderQueries();
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
        this.localStore.setCurrentQuery(query);
      },
      removeFavQuery(index, queryName) {
        FavQueriesSettings.removeFavQuery(queryName, this.currentKeyspace);
        this.favQueries.splice(index, 1);
      },
      renderQueries() {
        const savedQueries = this.$refs.favQuery;
        savedQueries.forEach((q) => {
          if (q.style.display !== 'none') {
            this.codeMirror = GraqlCodeMirror.getCodeMirror(q);
            this.codeMirror.setOption('readOnly', 'nocursor');
          }
        });
      },
    },
  };
</script>
