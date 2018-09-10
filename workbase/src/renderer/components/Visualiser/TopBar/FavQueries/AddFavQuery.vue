<template>
    <div class="add-fav-query">
        <!--<div @click="$emit('close-add-query-panel')"><vue-icon class="close-container" icon="cross" iconSize="12" className="tab-icon"></vue-icon></div>-->
        <div class="panel-body">
            <vue-input class="query-name-input" placeholder="Query name" v-on:input-changed="updateCurrentQueryName" className="vue-input"></vue-input>
            <vue-button v-on:clicked="addFavQuery" icon="floppy-disk" className="vue-button save-query-btn"></vue-button>
        </div>
        <div class="editor-tab">
            <div @click="$emit('close-add-query-panel')"><vue-icon icon="cross" iconSize="12" className="tab-icon"></vue-icon></div>
        </div>
    </div>
</template>

<script>

  import FavQueriesSettings from './FavQueriesSettings';

  export default {
    name: 'AddFavQuery',
    props: ['currentQuery', 'currentKeyspace'],
    data() {
      return {
        currentQueryName: '',
      };
    },
    methods: {
      updateCurrentQueryName(val) {
        this.currentQueryName = val;
      },
      addFavQuery() {
        this.$emit('close-add-query-panel');
        FavQueriesSettings.addFavQuery(
          this.currentQueryName,
          this.currentQuery,
          this.currentKeyspace,
        );
        this.$emit('refresh-queries');
        this.currentQueryName = '';
      },
    },
  };
</script>

<style scoped>

    .close-container{
        position: absolute;
        right: 0px;
        top:0px;
        z-index: 1;
    }

    .add-fav-query {
        background-color: var(--gray-2);
        border: var(--container-darkest-border);
        width: 100%;
        margin-top: 10px;
        max-height: 142px;
        position: relative;
        display: flex;
        flex-direction: row;
    }

    .panel-body {
        display: flex;
        flex-direction: row;
        align-items: center;
        width: 100%;
        padding-left: 10px;
        padding-top: 10px;
        padding-bottom: 10px;
    }

    .query-name-input {
        width: 100%;
        margin-right: var(--element-margin);
    }

    .editor-tab {
        height: 54px;
        width: 13px;
        flex-direction: column;
        display: flex;
        position: relative;
        float: right;
        border-left: var(--container-light-border);
        margin-left: var(--element-margin);
    }


</style>
