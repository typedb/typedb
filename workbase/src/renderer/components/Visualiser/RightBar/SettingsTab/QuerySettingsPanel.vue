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
                <input class="input-small panel-value query-limit-input" type="number" v-model="queryLimit">
            </div>
            <div class="panel-content-item">
                <h1 class="panel-label">Neighbour Limit:</h1>
                <input class="input-small panel-value neighbour-limit-input" type="number" v-model="neighboursLimit">
            </div>
            <div class="panel-content-item">
                <h1 class="panel-label">Load Roleplayers:</h1>
                <div class="panel-value load-roleplayers-switch"><vue-switch :isToggled="loadRolePlayers" v-on:toggled="updateLoadRoleplayers"></vue-switch></div>
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
        queryLimit: QueryUtils.getQueryLimit(),
        neighboursLimit: QueryUtils.getNeighboursLimit(),
        loadRolePlayers: QueryUtils.getRolePlayersStatus(),
      };
    },
    watch: {
      queryLimit(newVal) {
        QueryUtils.setQueryLimit(newVal);
      },
      neighboursLimit(newVal) {
        QueryUtils.setNeighboursLimit(newVal);
      },
    },
    methods: {
      toggleContent() {
        this.showQuerySettings = !this.showQuerySettings;
      },
      updateLoadRoleplayers(newVal) {
        QueryUtils.setRolePlayersStatus(newVal);
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
