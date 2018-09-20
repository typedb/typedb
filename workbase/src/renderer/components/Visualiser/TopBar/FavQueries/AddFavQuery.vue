<template>
    <div class="add-fav-query z-depth-3">
        <div class="panel-body">
            <vue-tooltip content="please write a query name" :isOpen="showAddFavQueryToolTip" :usePortal="false" :child="queryNameInput" v-on:close-tooltip="$emit('toggle-fav-query-tooltip', false)"></vue-tooltip>
            <vue-button v-on:clicked="addFavQuery" icon="floppy-disk" className="vue-button save-query-btn"></vue-button>
        </div>
        <div class="editor-tab">
            <div @click="$emit('close-add-query-panel')"><vue-icon icon="cross" iconSize="12" className="tab-icon"></vue-icon></div>
        </div>
    </div>
</template>

<script>
  import { InputGroup } from '@blueprintjs/core';

  import React from 'react';

  import FavQueriesSettings from './FavQueriesSettings';

  export default {
    name: 'AddFavQuery',
    props: ['currentQuery', 'currentKeyspace', 'showAddFavQueryToolTip', 'favQueries'],
    data() {
      return {
        currentQueryName: '',
        queryNameInput: null,
      };
    },
    created() {
      this.renderQueryNameInput();
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
      renderQueryNameInput() {
        this.queryNameInput = React.createElement(InputGroup, {
          className: 'vue-input query-name-input',
          placeholder: 'Query name',
          type: 'text',
          onChange: (val) => { this.updateCurrentQueryName(val.target.value); },
        });
      },
    },
  };
</script>

<style scoped>


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
