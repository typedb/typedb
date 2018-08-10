<template>
  <transition name="slide-fade" appear> 
    <div class="wrapper">
        <div>
            <div class="title">Settings</div>
            <nav class="nav" role="navigation">
            <ul class="nav-list">
                <li v-bind:id="tab" :class="{'active':activeTab===tab}" v-for="tab in tabs" @click="activeTab=tab" :key="tab">{{tab}}</li>
            </ul>
            </nav>
            <component :is="activeTab"></component>
        </div>
    </div>
  </transition>
</template>
<style scoped>
.slide-fade-enter-active {
    transition: all .8s ease;
}
.slide-fade-enter,
.slide-fade-leave-active {
    opacity: 0;
}

.wrapper{
  display: flex;
  flex-direction: row;
  justify-content: center;
  padding-top:30px;
}    

.title{
  display: flex;
  justify-content: center;
  font-size:120%;
  font-weight: bold;
  margin: 10px;
}
.nav-list{
  display: flex;
  flex-direction: row;
  justify-content: center;
}
.active{
  border-bottom: 2px solid #0674D7;
}

li{
  cursor: pointer;
  margin: 30px;
}
</style>

<script>
  import Engine from './tabs/EngineTab.vue';
  import Preferences from './tabs/PreferencesTab.vue';
  
  export default {
    name: 'SettingsPanel',
    components: {
      Engine,
      Preferences,
    },
    data() {
      const tabsArray = ['Engine', 'Preferences'];
      return {
        tabs: tabsArray,
        activeTab: tabsArray[0],
      };
    },
  };
</script>