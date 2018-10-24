<template>
    <div class="add-fav-query z-depth-3">
        <div class="panel-body">

            <input class="input query-name-input" placeholder="Query name" v-model="currentQueryName">
            <tool-tip class="fav-query-name-tooltip" :isOpen="showAddFavQueryToolTip" msg="Please write a query name" arrowPosition="top" v-on:close-tooltip="$emit('toggle-fav-query-tooltip', false);"></tool-tip>

            <button @click="addFavQuery" class="btn save-query-btn"><vue-icon icon="floppy-disk" className="vue-icon"></vue-icon></button>
        </div>
        <div class="editor-tab">
            <div class="close-add-fav-query-container" @click="$emit('close-add-query-panel')"><vue-icon icon="cross" iconSize="12" className="tab-icon"></vue-icon></div>
        </div>
    </div>
</template>

<script>

  import FavQueriesSettings from './FavQueriesSettings';
  import ToolTip from '../../../UIElements/ToolTip';

  export default {
    name: 'AddFavQuery',
    components: { ToolTip },
    props: ['currentQuery', 'currentKeyspace', 'showAddFavQueryToolTip', 'favQueries'],
    data() {
      return {
        currentQueryName: '',
        queryNameInput: null,
      };
    },
    watch: {
      currentQueryName() {
        this.$emit('toggle-fav-query-tooltip');
      },
    },
    methods: {
      updateCurrentQueryName(val) {
        this.currentQueryName = val;
      },
      addFavQuery(event) {
        if (event.stopPropagation) event.stopPropagation();

        const favQueryNames = this.favQueries.map(x => x.name);

        if (favQueryNames.includes(this.currentQueryName)) {
          this.$notifyInfo('Query name already saved. Please choose a different name.');
        } else if (this.currentQueryName === '') {
          this.$emit('toggle-fav-query-tooltip', true);
        } else if (!this.currentQuery.length) {
          this.$notifyInfo('Please type in a query.');
        } else {
          this.$emit('close-add-query-panel');

          FavQueriesSettings.addFavQuery(
            this.currentQueryName,
            this.currentQuery,
            this.currentKeyspace,
          );
          this.$emit('refresh-queries');
          this.currentQueryName = '';
          this.$notifyInfo('New query saved!');
        }
      },
    },
  };
</script>

<style scoped>

    .query-name-input {
        width: 565px !important;
        margin-right: var(--element-margin) !important;
    }

    .fav-query-name-tooltip {
        position: absolute;
        top: 55px;
        right: 250px;
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
