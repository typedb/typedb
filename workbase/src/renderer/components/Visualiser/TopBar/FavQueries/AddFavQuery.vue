<template>
    <div class="add-fav-query">
        <div @click="$emit('close-add-query-panel')"><vue-icon class="close-container" icon="cross" iconSize="12" className="tab-icon"></vue-icon></div>
        <div class="panel-body">
            <h1 class="label">Save Query</h1>
            <vue-input class="query-name-input" placeholder="query name" v-on:input-changed="updateCurrentQueryName"></vue-input>
            <vue-button v-on:clicked="addFavQuery" text="save" className="vue-button save-query-btn"></vue-button>
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
        padding-top: 1px;
        z-index: 1;
    }

    .add-fav-query {
        background-color: var(--gray-2);
        padding: 10px;
        border: var(--container-darkest-border);
        width: 100%;
        margin-top: 20px;
        max-height: 142px;
        position: relative;
    }

    .panel-body {
        display: flex;
        flex-direction: row;
        align-items: center;
        width: 100%;
    }

    .label {
        width: 68px;
    }

    .query-name-input {
        width: 100%;
        margin-left: 5px;
        margin-right: 5px;
    }



</style>
