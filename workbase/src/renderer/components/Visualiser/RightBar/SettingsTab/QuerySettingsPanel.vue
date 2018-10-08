<template>
    <div class="panel-container noselect">
        <div @click="toggleContent" class="panel-header">
            <vue-icon :icon="(showQuerySettings) ?  'chevron-down' : 'chevron-right'" iconSize="14" className="vue-icon"></vue-icon>
            <h1>Query Settings</h1>
        </div>
        <div v-show="showQuerySettings">

        <div class="panel-content">
            <div class="panel-content-item">
                <h1 class="panel-label">Query Limit:</h1>
                <div class="panel-value"><vue-input :defaultValue="queryLimit" v-on:input-changed="updateQueryLimit" className="vue-input vue-input-small"></vue-input></div>
            </div>
            <div class="panel-content-item">
                <h1 class="panel-label">Neighbour Limit:</h1>
                <div class="panel-value"><vue-input :defaultValue="neighboursLimit" v-on:input-changed="updateNeighboursLimit" className="vue-input vue-input-small"></vue-input></div>
            </div>
            <div class="panel-content-item">
                <h1 class="panel-label">Load Roleplayers:</h1>
                <div class="panel-value"><vue-switch :defaultChecked="loadRolePlayers" v-on:switch-changed="toggleAutoloadRoleplayers" className="vue-input vue-input-small"></vue-switch></div>
            </div>
        </div>
        </div>
    </div>
</template>

<script>

  import QueryUtils from './QuerySettings';

  export default {

    name: 'QuerySettings',
    data() {
      return {
        showQuerySettings: true,
      };
    },
    computed: {
      queryLimit() {
        return QueryUtils.getQueryLimit();
      },
      neighboursLimit() {
        return QueryUtils.getNeighboursLimit();
      },
      loadRolePlayers() {
        return QueryUtils.getRolePlayersStatus();
      },
    },
    methods: {
      toggleContent() {
        this.showQuerySettings = !this.showQuerySettings;
      },
      toggleAutoloadRoleplayers(newVal) {
        QueryUtils.setRolePlayersStatus(newVal);
      },
      updateQueryLimit(newVal) {
        QueryUtils.setQueryLimit(newVal);
      },
      updateNeighboursLimit(newVal) {
        QueryUtils.setNeighboursLimit(newVal);
      },
    },
  };
</script>

<style scoped>

    .panel-content {
        padding: var(--container-padding);
        border-bottom: var(--container-darkest-border);
        display: flex;
        flex-direction: column;
        z-index: 10;
    }

    .panel-content-item {
        padding: var(--container-padding);
        display: flex;
        flex-direction: row;
        align-items: center;
        height: var(--line-height);
    }

    .panel-label {
        width: 90px;
    }

    .panel-value {
        width: 100px;
        justify-content: center;
        display: flex;
    }

</style>
