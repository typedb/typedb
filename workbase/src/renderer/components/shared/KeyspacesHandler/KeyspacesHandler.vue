<template>
<div class="keyspaces-wrapper">
     <div class="dropdown">
         <div class="wrap-button" @click="(isGraknRunning) ? toggleToolTip() : false" :class="{'disabled':!isGraknRunning}">
            <div id="keyspaces" class="selector-button">{{(currentKeyspace !== null) ? currentKeyspace : 'keyspace'}}</div>
            <caret-icon :toggleNorth="toolTipShown === 'keyspaces'"></caret-icon>
        </div>
         <transition name="slide-fade">
            <ul id="keyspaces-list" class="keyspaces-list z-depth-3" v-show="toolTipShown === 'keyspaces'">
                <li :id="ks" class="ks-key" v-for="ks in keyspaces" :key="ks" @click="setKeyspace(ks)">{{ks}}</li>
            </ul>
        </transition>  
    </div> 
</div>
</template>

<style scoped>

.keyspaces-wrapper{
    padding: 5px;
    display: flex;
}

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

.wrap-button{
    cursor: pointer;
    display: inline-flex;
    height: 37px;
    align-items: center;
    border: 2px solid rgb(58, 58, 58);
    border-radius: 6%;
    padding: 5%;
}

.keyspaces-wrapper{
    padding: 5px;
    display: flex;
    position: absolute;
}

.selector-button {
    color: #00eca2;
    font-size: 100%;
    margin-right: 10px;
}

li {
    border-bottom: 2px solid rgb(58, 58, 58);
}

li:hover {
    color: #00eca2;
}

.keyspaces-list {
    position: absolute;
    top: 100%;
    margin-top: 5px;
    padding: 5px 10px;
    right:5px;
    background-color: #282828; 
    border-radius: 6%;
}

.ks-key {
    position: relative;
    cursor: pointer;
    padding: 10px 5px;
    border-radius: 3px;
    margin: 3px 0px;
}
</style>

<script>
import CaretIcon from '@/components/UIElements/CaretIcon.vue';

import { CURRENT_KEYSPACE_CHANGED } from '../StoresActions';

export default {
  name: 'KeyspacesList',
  props: ['localStore', 'toolTipShown'],
  components: { CaretIcon },
  computed: {
    keyspaces() { return this.$store.getters.allKeyspaces; },
    currentKeyspace() { return this.localStore.getCurrentKeyspace(); },
    isGraknRunning() { return this.$store.getters.isGraknRunning; },
  },
  watch: {
    keyspaces(val) {
      // If user deletes current keyspace from Keyspaces page, set new current keyspace to null
      if (!val.includes(this.currentKeyspace)) { this.localStore.dispatch(CURRENT_KEYSPACE_CHANGED, null); }
    },
    isGraknRunning(val) {
      if (!val) {
        this.$notifyInfo('It was not possible to retrieve keyspaces <br> - make sure Grakn is running <br> - check that host and port in Grakn URI are correct', 'top-center');
      }
    },
  },
  methods: {
    setKeyspace(name) {
      this.localStore.dispatch(CURRENT_KEYSPACE_CHANGED, name);
      this.$emit('toggle-tool-tip');
    },
    toggleToolTip() {
      if (!(this.toolTipShown === 'keyspaces')) {
        this.$emit('toggle-tool-tip', 'keyspaces');
      } else {
        this.$emit('toggle-tool-tip');
      }
    },
  },
};
</script>
