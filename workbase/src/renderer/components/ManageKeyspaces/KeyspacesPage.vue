<template>
  <transition name="slide-fade" appear>
    <div class="wrapper">
      <div class="left-column">
        <create-keyspace-card></create-keyspace-card>
          <!-- <import-card :keyspaces="keyspaces"></import-card> -->
      </div>
      <div class="central-column">
          <div class="title">Keyspaces</div>
          <div class="list-container">
              <div class="ks-div noselect" v-for="ks in keyspaces" :key="ks">
                  <div class="left-side keyspace-label">{{ks}}</div>
                  <div class="right-side">
                      <!-- <img @click="showSaveDialog(ks)" src="static/img/icons/icon_export_white.svg" width="19"> -->
                      <i v-bind:id="'delete-'+ks" @click="askConfirmation(ks)" class="fas fa-trash-alt"></i>
                  </div>
              </div>
          </div>
      </div>
      <div class="right-column"></div>
    </div>
  </transition>
</template>

<style scoped>

.fa-trash-alt{
  cursor: pointer;
}

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
  padding-top: 30px;
}

.ks-div{
    display: flex;
    justify-content: space-between;
    margin: 5px 0px;
    background-color: #282828;
    padding: 5px 0px;
}


.left-side{
    display: flex;
    align-items: flex-start;
    margin-left:10px;
}

.right-side{
    display: flex;
    align-items: center;
    margin-right: 10px;
}

.list-container{
    display: flex;
    flex-direction: column;
    padding: 10px;
}

.left-column{
    display: flex;
    flex: 1;
    flex-direction: column;
    padding-left: 5px;
}

.right-column{
    display: flex;
    flex: 1;
    flex-direction: column;
}
.central-column{
    display: flex;
    flex: 2;
    flex-direction: column;
}

img{
    cursor: pointer;
}

.title{
  display: flex;
  justify-content: center;
  font-size:120%;
  font-weight: bold;
  margin:10px 0px;
}
.nav-list{
  display: flex;
  flex-direction: row;
}
.active{
  border-bottom: 2px solid #0674D7;
}
.nav-list{
  display: flex;
  justify-content: center;
}
li{
  cursor: pointer;
  margin: 5px;
}
</style>

<script>
import { mapGetters } from 'vuex';

import ImportCard from './ImportCard.vue';
import CreateKeyspaceCard from './CreateKeyspaceCard.vue';
import PanelContainer from '../shared/PanelContainer.vue';
// import Exporter from './Exporter/Exporter';
const { dialog } = require('electron').remote;

export default {
  name: 'KeyspacesPage',
  components: { ImportCard, CreateKeyspaceCard, PanelContainer },
  computed: mapGetters({ keyspaces: 'allKeyspaces' }),
  methods: {
    deleteKeyspace(name) {
      this.$store.dispatch('deleteKeyspace', name)
        .then(() => { this.$notifySuccess(`Keyspace [${name}] successfully deleted`); })
        .catch((error) => { this.$notifyError(error); });
    },
    askConfirmation(name) {
      this.$notifyConfirmDelete(`Confirm deletion of keyspace [${name}]`, () => { this.deleteKeyspace(name); });
    },
    showSaveDialog(name) {
      dialog.showSaveDialog({
        message: 'Select location for the exported keyspace file',
        defaultPath: `~/Desktop/${name}.gql`,
      },
      (targetPath) => { if (!targetPath) return; this.exportToFile(name, targetPath); });
    },
    // exportToFile(name, targetPath) {
    //   try {
    //     // Exporter.exportKeyspace(name, targetPath);
    //     this.$notifySuccess(`Keyspace [${name}] successfully exported.`);
    //   } catch (err) {
    //     this.$notifyError(err);
    //   }
    // },
  },
};
</script>
