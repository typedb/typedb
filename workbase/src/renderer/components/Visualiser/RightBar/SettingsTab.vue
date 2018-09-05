<template>
    <div class="panel-container">
        <div class="settings-header">
            <h1>SETTINGS</h1>
        </div>
        <div class="content" v-show="currentKeyspace">
            <div class="content-item">
                <h1 class="label">LIMIT QUERY</h1>
                <div class="value"><vue-input :defaultValue="queryLimit" v-on:input-changed="updateQueryLimit"></vue-input></div>
            </div>
            <div class="content-item">
                <h1 class="label">LIMIT NEIGHBOURS</h1>
                <div class="value"><vue-input :defaultValue="neighboursLimit" v-on:input-changed="updateNeighboursLimit"></vue-input></div>
            </div>
            <div class="content-item">
                <h1 class="label">AUTOLOAD ROLEPLAYERS</h1>
                <div class="value"><vue-switch :defaultChecked="loadRolePlayers" v-on:switch-changed="toggleAutoloadRoleplayers"></vue-switch></div>
            </div>
        </div>
    </div>
</template>

<script>

  import QueryUtils from './QuerySettings';

  export default {

    name: 'SettingsTab',
    props: ['localStore'],
    data() {
      return {
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

    .settings-header {
        padding: var(--container-padding);
        width: 100%;
        height: 20px;
        background-color: var(--gray-3);
        align-items: center;
        display: flex;
        justify-content: center;

        -webkit-touch-callout: none; /* iOS Safari */
        -webkit-user-select: none; /* Safari */
        -khtml-user-select: none; /* Konqueror HTML */
        -moz-user-select: none; /* Firefox */
        -ms-user-select: none; /* Internet Explorer/Edge */
        user-select: none; /* Non-prefixed version, currently supported by Chrome and Opera */
    }

    .content {
        padding: var(--container-padding);
        display: flex;
        flex-direction: column;
        max-height: 120px;
    }

    .content-item {
        padding: var(--container-padding);
        display: flex;
        flex-direction: row;
        align-items: center;
        position: relative;
        height: 35px;
    }

    .label {
        margin-right: 20px;
        width: 134px;
    }

    .value {
        width: 35px;
        justify-content: center;
        display: flex;
    }

</style>
