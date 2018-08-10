<template>
<div>
    <button @click="togglePanel" class="btn btn-default console-button" :class="{'disabled':!currentKeyspace, 'btn-selected':toolTipShown === 'querySettings'}">
      <i id="cog" class="fa fa-cog rotate"></i>
    </button>
    <div class="query-settings-wrapper">
    <transition name="slide-fade">
        <div v-if="toolTipShown === 'querySettings'" class="dropdown-content" id="query-settings">
            <div class="panel-heading">
                <h4><i class="page-header-icon fa fa-cog"></i>Query settings
                <a @click="togglePanel"><i class="page-header-close-icon fa fa-times"></i></a></h4>
            </div>
            <div class="panel-body">
                <div class="divide"></div>
                <div class="dd-item">
                  <div class="left">Limit query:</div><div class="right"><input id="limit-query" v-model="queryLimit" type="text" class="form-control limit" maxlength="3" size="4"></div>
                </div>
                <div class="divide"></div>
                  <div class="dd-item">
                    <div class="left">Limit neighbours:</div><div class="right"><input id="limit-neighbours" v-model="neighboursLimit" type="text" class="form-control limit" maxlength="3" size="4"></div>
                </div>
                <div class="divide"></div>
                  <div class="dd-item">
                    <div class="left"><input id="load-role-players" type="checkbox" v-model="loadRolePlayers"></div><div class="right">Autoload role players</div>
                </div>
            </div>
        </div>
    </transition>
    </div>
</div>
</template>

<style scoped>

.slide-fade-enter-active {
    transition: all .6s ease;
}
.slide-fade-leave-active {
    transition: all .3s cubic-bezier(1.0, 0.5, 0.8, 1.0);
}
.slide-fade-enter,
.slide-fade-leave-active {
    transform: translateY(-10px);
    opacity: 0;
}

.disabled{
    opacity:0.5;
    cursor: default;
}

.rotate{
    -moz-transition: all 0.4s linear;
    -webkit-transition: all 0.4s linear;
    transition: all 0.4s linear;
}

.down{
    -ms-transform: rotate(90deg);
    -moz-transform: rotate(90deg);
    -webkit-transform: rotate(90deg);
    transform: rotate(90deg);
}

.query-settings-wrapper {
  position: absolute;
}

.dropdown-content {
    position: relative;
    top: 100%;
    z-index: 2;
    margin-top: 5px;
    background-color: #282828;
    padding: 10px;
    width: 260px;
    right: 200px;
}

.panel-heading {
    margin-bottom: 10px;
}

.page-header-icon {
    padding-right: 45px;
    font-size: 20px;
}

.page-header-close-icon {
    float: right;
    cursor: pointer;
}

.panel-body {
    display: flex;
    flex-direction: column;
}

.dd-item {
    display: inline;
    margin: 10px 10px;
    padding-left: 7px;
}

.divide {
  border-bottom: 1px solid #606060;
  margin-top: 5px;
  margin-bottom: 5px;
}

.left{
  display: inline-flex;
  margin-right: 5px;
}

.right{
  display: inline-flex;
  flex:1;
}

.input-box {
  border: 1px solid #ccc;
  float: right;
}

.rp-settings {
  padding-bottom: 10px;
  padding-top: 10px;
}

.limit {
  color: black;
}

</style>

<script>
import Utils from '../QuerySettings';

export default {
  name: 'QuerySettings',
  props: ['currentKeyspace', 'toolTipShown'],
  data() {
    return {
      // useReasoner: User.getReasonerStatus(),
      showSettings: false,
      queryLimit: Utils.getQueryLimit(),
      loadRolePlayers: Utils.getRolePlayersStatus(),
      neighboursLimit: Utils.getNeighboursLimit(),
    };
  },
  watch: {
    // useReasoner(newVal, oldVal) {
    //   User.setReasonerStatus(newVal);
    //   if (!newVal) this.materialiseReasoner = false;
    // },
    queryLimit(newVal) {
      Utils.setQueryLimit(newVal);
      if (newVal.length > 0) this.queryLimit = Utils.getQueryLimit();
    },
    // Set auto-loading of Role-Players status
    loadRolePlayers(newVal) {
      Utils.setRolePlayersStatus(newVal);
    },
    // Neighbours limit from neighbour settings
    neighboursLimit(newVal) {
      Utils.setNeighboursLimit(newVal);
      if (newVal.length > 0) this.neighboursLimit = Utils.getNeighboursLimit();
    },
  },
  methods: {
    togglePanel() {
      if (!(this.toolTipShown === 'querySettings')) {
        this.$emit('toggle-tool-tip', 'querySettings');
        document.getElementById('cog').classList.toggle('down');
      } else {
        this.$emit('toggle-tool-tip');
        this.showRelationshipSettings = false;
        document.getElementById('cog').classList.toggle('down');
      }
    },
  },
};
</script>
