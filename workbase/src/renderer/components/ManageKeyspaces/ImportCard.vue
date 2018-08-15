<template>
    <div class=" noselect">
       <!-- <div class="sub-title">Import from file</div> -->
        <div class="card-panel-list noselect">
            <div class="line">
                <input class="grakn-input" type="text" maxlength="" :value="fileName" disabled>
                <div @click="openDialog" class="btn left-line small">Choose</div>
            </div>
            <div class="noselect">
                <font style="color:white;">Import to keyspace:</font>
                <div @click="showKeyspacesList=!showKeyspacesList" class="select-div">
                    <div class="select-div-line">
                        <span style="margin-left:auto;">{{ksSelected}}</span>
                        <caret-icon :toggleNorth="showKeyspacesList"></caret-icon>
                    </div>
                    <div v-show="showKeyspacesList" class="card-panel-list inner-card">
                        <div class="existing-types-list card">
                            <ul>
                            <li v-for="ks in keyspaces" @click="selectKs(ks)" :key="ks">{{ks}}</li>
                            </ul>
                        </div>
                    </div>
                </div>
            </div>
            <loading-button :clickFunction="executeImport" value="Import" :isLoading="isLoading" :disabled="!ksSelected || !(filePath.length) || isLoading"></loading-button>
        </div>
    </div>
</template>
<script>
import CaretIcon from '../UIElements/CaretIcon.vue';

const { dialog } = require('electron').remote;
const fs = require('fs');

export default {
  name: 'ImportCard',
  props: ['keyspaces'],
  components: { CaretIcon },
  data() {
    return {
      filePath: '',
      isLoading: false,
      showKeyspacesList: false,
      ksSelected: undefined,
    };
  },
  computed: {
    fileName() {
      if (!this.filePath) return 'No file chosen';
      const filePathArray = this.filePath.split('/');
      return filePathArray[(filePathArray.length - 1)];
    },
  },
  methods: {
    openDialog() {
      dialog.showOpenDialog({ filters: [{ name: 'graql', extensions: ['gql'] }] }, (fileNames) => {
        // fileNames is an array that contains all the selected
        if (fileNames === undefined) {
          return;
        }
        this.filePath = fileNames[0];
      });
    },
    executeImport() {
      // If already loading an schema don't try to submit twice
      if (this.isLoading) return;
      this.isLoading = true;
      const query = fs.readFileSync(this.filePath, 'utf-8');
      const keyspace = this.ksSelected;

      this.$store.dispatch('importFromFile', { query, keyspace })
        .then(() => { this.$notifySuccess(`Data imported into [${keyspace}] successfully!`); })
        .catch((error) => { this.$notifyError(error, 'Importing from file'); })
        .then(() => { this.resetPanel(); });
    },
    resetPanel() {
      this.ksSelected = undefined;
      this.filePath = '';
      this.isLoading = false;
    },
    selectKs(name) {
      this.ksSelected = name;
    },
  },
};
</script>

<style scoped>

.noselect{
    margin-bottom: 20px;
}

.select-div-line{
    display: flex;
    justify-content: space-between;
    align-items: center;
}

.select-div{
    background-color: #2E2D2D;
    display:flex; 
    flex-flow: column;
    cursor:pointer;
    position: relative;
    margin-top: 5px;
}

.card-panel{
    background-color: #282828;
    display: flex;
    flex-direction: column;
    margin-top:10px;
    margin-bottom:0px;
}

.small{
    font-size: 80%;
    padding: 4px;
    height: auto;
    line-height: 20px;
}

.sub-title{
    margin:10px 0px;
}
.line{
    margin-bottom:20px;
    margin-top: 10px;
    display: flex;
    justify-content: space-between;
    align-items: flex-end;
}
.left-line{
    display: flex;
    flex: 1;
}
.right-line{
    display: flex;
    flex: 1;
}
.card-panel-list {
    padding: 8px;
    margin-bottom: 10px;
    display: flex;
    flex-direction: column;
}
.card-panel {
    padding: 5px;
    margin-bottom: 10px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    color:white;
}
li{
  cursor: pointer;
  margin: 2px;
}

li:hover{
  background-color: #404040;
}

.existing-types-list{
  background-color: #262626;
  padding: 5px;
}
</style>

