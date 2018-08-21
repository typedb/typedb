<template>
  <div v-show="showEditToolTip" class="arrow_box">
    <div class="line">
    <span @click="$emit('hide-tool-tip')" class="close-edit noselect">x</span>
    </div>
    <nav class="nav" role="navigation" v-if="relatesToolTip">
      <ul class="nav-list">
        <li class="nav-list__item" :class="{'active':activeTab===tab}" v-for="tab in tabs" @click="activeTab=tab" :key="tab">{{tab}}</li>
      </ul>
    </nav>
    <div class="body">
      <existing-list v-if="activeTab === 'Exisiting Role' || !relatesToolTip"
        :localStore="localStore" 
        :type="type"
        :typesAlreadySelected="typesAlreadySelected">
      </existing-list>
      <new-role v-if="activeTab==='New Role'" class="new-role"
        ref="typeComponent" 
        :localStore="localStore">
      </new-role>
    </div>
  </div>
</template>

<style scoped>

.line{
  display: flex;
  justify-content: flex-end;
}

.close-edit{
  padding-right: 4px;
  font-size: 105%;
  height: 15px;
  cursor: pointer;
  margin-left: auto;
  display: flex;
}

.arrow_box{
    position: absolute;
    right: 105%;    
    width: 120%;
    background-color: #282828;
    padding: 5px;
}

.arrow_box:after, .arrow_box:before {
	left: 100%;
	top: 10px;
	border: solid transparent;
	content: " ";
	height: 0;
	width: 0;
	position: absolute;
	pointer-events: none;
}

.arrow_box:after {
  border-color: rgba(46, 45, 45, 0);
	border-left-color: #282828;
	border-width: 8px;
	margin-top: -8px;
}
.arrow_box:before {
	border-color: rgba(46, 45, 45, 0);
	border-left-color: #282828;
	border-width: 9px;
	margin-top: -9px;
}

.nav-list {
  display: flex;
  justify-content: space-around;
  padding: 0;
  list-style-type: none;
  width: 100%;
}

.nav-list__item {
  border-bottom: 2px solid transparent;
  color: #B0B0B2;
  cursor: pointer;
  padding-bottom: 5px;
  position: relative;
  text-align: center;
  user-select: none;
}

.nav-list__item:hover {
  border-color: #0674D7;
  background-color: #282828;
}

.nav-list__item.active {
  border-color: #0674D7;
  color:white;
}

.new-role {
  margin-top: 5%;
}

li:hover{
  background-color: #404040;
}

li{
  cursor: pointer;
  margin: 4px;
}
</style>
<script>

import ExistingList from './ExistingList';
import NewRole from './NewRole.vue';

export default {
  name: 'EditToolTip',
  props: ['localStore', 'typesAlreadySelected', 'showEditToolTip', 'relatesToolTip', 'type'],
  components: { ExistingList, NewRole },
  data() {
    const tabsArray = ['Exisiting Role', 'New Role'];
    return {
      tabs: tabsArray,
      activeTab: tabsArray[0],
    };
  },
};
</script>

