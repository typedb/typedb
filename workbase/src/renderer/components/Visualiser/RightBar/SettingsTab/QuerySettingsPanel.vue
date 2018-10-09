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
                <div class="panel-value load-roleplayers-switch"><label class="switch"><input type="checkbox" v-model="loadRolePlayers"><span class="slider round"></span></label></div>
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
      loadRolePlayers(newVal) {
        QueryUtils.setRolePlayersStatus(newVal);
      },
    },
    methods: {
      toggleContent() {
        this.showQuerySettings = !this.showQuerySettings;
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

    /* The switch - the box around the slider */
    .switch {
        position: relative;
        display: inline-block;
        width: 40px;
        height: 18px;
    }

    /* Hide default HTML checkbox */
    .switch input {display:none;}

    /* The slider */
    .slider {
        position: absolute;
        cursor: pointer;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        background-color: #ccc;
        -webkit-transition: .4s;
        transition: .4s;
    }

    .slider:before {
        position: absolute;
        content: "";
        height: 12px;
        width: 12px;
        left: 4px;
        bottom: 2px;
        background-color: var(--gray-5);
        -webkit-transition: .4s;
        transition: .4s;
    }

    input:checked + .slider {
        background-color: var(--green-4);
    }

    input:focus + .slider {
        box-shadow: 0 0 1px #2196F3;
    }

    input:checked + .slider:before {
        -webkit-transform: translateX(18px);
        -ms-transform: translateX(18px);
        transform: translateX(18px);
    }

    /* Rounded sliders */
    .slider.round {
        border-radius: 50px;
        border: var(--container-darkest-border);
        background-color: var(--gray-1);

    }

    .slider.round:before {
        border-radius: 50%;
    }

</style>
