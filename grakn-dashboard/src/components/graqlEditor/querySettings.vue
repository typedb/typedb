<!--
Grakn - A Distributed Semantic Database
Copyright (C) 2016  Grakn Labs Limited

Grakn is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

Grakn is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>. -->


<template>
<div>
    <button @click="togglePanel" class="btn btn-default"><i class="fa fa-cog"></i></button>
    <transition name="fade-in">
        <div v-if="showSettings" class="dropdown-content">
          <div class="panel-heading">
              <i class="page-header-icon fa fa-cog"></i>Query settings
              <a @click="closeSettings"><i class="page-header-close-icon fa fa-times"></i></a>
          </div>
          <div class="panel-body">
            <div class="dd-item">
                <div class="left"><input type="checkbox" v-model="freezeNodes"></div><div class="right">Lock nodes position</div>
            </div>
            <div class="divide"></div>
            <div class="dd-item">
                <div class="left"><input type="checkbox" v-model="useReasoner"></div><div class="right"> Activate inference</div>
            </div>
            <div class="divide"></div>
            <div class="dd-item">
              <div class="left ">Query limit:</div><input v-model="queryLimit" type="text" class="form-control input-box" maxlength="3" size="4">
            </div>
            <div class="divide"></div>
            <div class="rel-settings">
              <button @click="toggleRelationshipSettings" class="btn rel-btn">Relationship Settings</button>
            </div>
            <transition name="fade-in">
              <div v-if="showRelationshipSettings">
                <div class="dd-item">
                  <div class="left rp-settings"><input type="checkbox" v-model="loadRolePlayers"></div><div class="right">Autoload role players</div>
                </div>
                <div class="dd-item" v-if="this.loadRolePlayers">
                  <div class="left rp-settings">Limit role players:</div><input v-model="rolePlayersLimit" type="text" class="form-control" maxlength="3" size="4">
                </div>
              </div>
            </transition>
          </div>
        </div>
    </transition>
</div>
</template>

<style scoped>

.dropdown-content {
    position: absolute;
    top: 100%;
    right: -5%;
    z-index: 2;
    margin-top: 5px;
    background-color: #0f0f0f;
    padding: 10px;
}

.panel-heading {
    margin-bottom: 10px;
    width: 200px;
}

.page-header-icon {
    padding-right: 35px;
    font-size: 20px;
}

.page-header-close-icon {
    margin-left: 35px;
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

.rel-settings{
  text-align: center;
}

.rel-btn{
  display: inline;
  height: 30px;
  padding-bottom: 35px;
}

.input-box {
  border: 1px solid #ccc;
  float: right;
}

.rp-settings {
  padding-bottom: 10px;
  padding-top: 10px;
}

input:focus { 
    background-color: #00eca2;
}

</style>

<script>
import User from '../../js/User';

export default {
  name: 'QuerySettings',
  props:['state'],
  data() {
    return {
      useReasoner: User.getReasonerStatus(),
      loadRolePlayers: User.getRolePlayersStatus(),
      showSettings: false,
      showRelationshipSettings: false,
      queryLimit: User.getQueryLimit(),
      rolePlayersLimit: User.getRolePlayersLimit(),
      freezeNodes: User.getFreezeNodes(),
      elementId:'4',
    };
  },
  created() {
      // Global key binding for locking/unlocking nodes
    window.addEventListener('keyup', (e) => {
      if (e.ctrlKey && e.keyCode === 76) {
        this.freezeNodes = !this.freezeNodes;
      }
    });
    this.state.eventHub.$on('show-new-navbar-element',(elementId)=>{if(elementId!=this.elementId)this.showSettings=false;});
  },

  mounted() {
    this.$nextTick(() => {
    });
  },

  watch: {

    useReasoner(newVal, oldVal) {
      User.setReasonerStatus(newVal);
    },

    //Set loading of Role-Players status
    loadRolePlayers(newVal, oldVal) {
      User.setRolePlayersStatus(newVal);
      if(newVal){
        toastr.success('Role-Players will be loaded with relationships.');
      } else {
        toastr.success('Role-Players will not be loaded with relationships.');
      }      
    },

    freezeNodes(newVal, oldVal) {
      User.setFreezeNodes(newVal);
      if (newVal) {
        visualiser.fixAllNodes();
        toastr.success('All nodes LOCKED.');
      } else {
        visualiser.releaseAllNodes();
        toastr.success('All nodes UNLOCKED.');
      }
    },

    queryLimit(newVal, oldVal) {
      User.setQueryLimit(newVal);
      if (newVal.length > 0) this.queryLimit = User.getQueryLimit();
    },

    // Set Role-Players limit from relationship settings
    rolePlayersLimit(newVal, oldVal) {
      User.setRolePlayersLimit(newVal);
      if (newVal.length > 0) this.rolePlayersLimit = User.getRolePlayersLimit();
    },
  },

  methods: {
    closeSettings() {
      this.showSettings = false;
      this.showRelationshipSettings = false;
    },

    togglePanel(){
        this.showSettings=!this.showSettings;
        this.showRelationshipSettings = false;
        if(this.showSettings) this.state.eventHub.$emit('show-new-navbar-element',this.elementId);
    },

    toggleRelationshipSettings() {
      this.showRelationshipSettings = !this.showRelationshipSettings;
      if(this.showRelationshipSettings) this.state.eventHub.$emit('show-new-navbar-element',this.elementId);
    }    
  },
};
</script>
