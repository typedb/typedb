<template>
<transition name="fade-in">
  <div v-show="showPanel" class="wrapper">
    <nav class="nav" role="navigation">
      <ul class="nav-list">
        <li class="nav-list__item" :class="{'active':activeTab===tab}" v-for="tab in tabs" @click="activeTab=tab" :key="tab">{{tab}}</li>
      </ul>
    </nav>
    <div class="body">
      <keep-alive>
        <component 
          :is="activeTab" 
          :instances="metaTypeInstances" 
          ref="typeComponent" 
          :localStore="localStore">
        </component>
      </keep-alive>
      <div class="add-line">
        <loading-button 
          :clickFunction="sendQuery" 
          value="Add" 
          :isLoading="isLoading">
        </loading-button>
      </div>
    </div>
  </div>
</transition>
</template>

<style scoped>

.wrapper{
  position: absolute;
  z-index: 5;
  left: 5px;
  top: 5px;
  background-color: #282828;
  max-height: 100%;
  width: 320px;
  border-radius: 2px;
}
a {
  text-decoration: none;
}

a:link,
a:visited {
  color: #B0B0B2;
}

a:hover,
a:active {
  color: #0674D7;
}

.flex-center {
  justify-content: center;
  flex: 2;
}

.flex-right {
  justify-content: flex-end;
}

.nav-list {
  display: flex;
  justify-content: space-around;
  padding: 0;
  margin-top:20px;
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
}

.nav-list__item.active {
  border-color: #0674D7;
  color:white;
}

.body {
  padding: 20px 5px;
  border-radius: 4px;
}


.add-line{
  display: flex;
  flex-direction: row;
  justify-content: flex-end;
  align-items: center;
  height: 40px;
}
</style>
<script>
import Entity from './types/entity.vue';
import Relationship from './types/relationship.vue';
import Attribute from './types/attribute.vue';

export default {
  name: 'NewTypePanel',
  props: ['localStore', 'showPanel'],
  components: {
    Entity,
    Relationship,
    Attribute,
  },
  data() {
    const tabsArray = ['Entity', 'Relationship', 'Attribute'];
    return {
      tabs: tabsArray,
      activeTab: tabsArray[0],
      isLoading: false,
    };
  },
  computed: {
    metaTypeInstances() { return this.localStore.getMetaTypeInstances(); },
  },
  methods: {
    sendQuery() {
      this.isLoading = true;

      this.$refs.typeComponent
        .insertType()
        .then(() => {
          this.$refs.typeComponent.$emit('clear-panel');
          this.$notifySuccess('Concept successfully added!');
        })
        .catch((error) => { this.$notifyError(error); })
        .then(() => {
          this.isLoading = false;
        });
    },
  },
};
</script>
