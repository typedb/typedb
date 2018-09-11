<template>
    <div class="panel-container">
        <div @click="toggleContent" class="panel-header">
            <vue-icon :icon="(showQuerySettings) ?  'chevron-down' : 'chevron-right'" iconSize="14"></vue-icon>
            <h1>Query Settings</h1>
        </div>
        <div class="content" v-show="showQuerySettings">
            <div class="content-item">
                <h1 class="label">Query Limit</h1>
                <div class="value"><vue-input :defaultValue="queryLimit" v-on:input-changed="updateQueryLimit" className="vue-input vue-input-small"></vue-input></div>
            </div>
            <div class="content-item">
                <h1 class="label">Neighbour Limit</h1>
                <div class="value"><vue-input :defaultValue="neighboursLimit" v-on:input-changed="updateNeighboursLimit" className="vue-input vue-input-small"></vue-input></div>
            </div>
            <div class="content-item">
                <h1 class="label">Load Roleplayers</h1>
                <div class="value load-roleplayers"><vue-switch :defaultChecked="loadRolePlayers" v-on:switch-changed="toggleAutoloadRoleplayers" className="vue-input vue-input-small"></vue-switch></div>
            </div>
        </div>
    </div>
</template>

<script>

  import QueryUtils from './QuerySettings';

  export default {

    name: 'QuerySettings',
    props: ['localStore'],
    data() {
      return {
        showQuerySettings: false,
        queryLimit: QueryUtils.getQueryLimit(),
        neighboursLimit: QueryUtils.getNeighboursLimit(),
        loadRolePlayers: QueryUtils.getRolePlayersStatus(),
      };
    },
    computed: {
      currentKeyspace() {
        return this.localStore.getCurrentKeyspace();
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
        if (newVal.length > 0) this.queryLimit = QueryUtils.getQueryLimit();
      },
      updateNeighboursLimit(newVal) {
        QueryUtils.setNeighboursLimit(newVal);
        if (newVal.length > 0) this.neighboursLimit = QueryUtils.getNeighboursLimit();
      },
    },
  };
</script>

<style scoped>

    .content {
        padding: var(--container-padding);
        border-bottom: var(--container-darkest-border);
        display: flex;
        flex-direction: column;
        max-height: 120px;
    }

    .content-item {
        padding: var(--container-padding);
        display: flex;
        flex-direction: row;
        align-items: center;
        height: var(--line-height);
    }

    .label {
        width: 90px;
    }

    .value {
        width: 55px;
        justify-content: center;
        display: flex;
        position: absolute;
        right: 10px;
    }

    .load-roleplayers {
        margin-right: -2px;
    }

</style>
