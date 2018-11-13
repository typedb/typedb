<template>
    <div class="keyspace-handler-panel">

        <button @click="showNewKeyspacePanel = true" class="btn"><vue-icon icon="plus" className="vue-icon"></vue-icon></button>
        <div v-show="showNewKeyspacePanel" class="new-keyspace-container z-depth-3">
            <div class="new-keyspace-header">
                <div class="preferences-title">Add New Keyspace</div>
                <div class="close-container" @click="showNewKeyspacePanel = false"><vue-icon icon="cross" iconSize="12" className="tab-icon"></vue-icon></div>
            </div>
            <div class="panel-content">
                <div class="label">Name</div>
                <div class="keyspace-name-input"><input class="input-small" v-model="keyspaceName"></div>
            </div>
            <div class="row">
                <button @click="showNewKeyspacePanel = false" class="btn">CANCEL</button>
                <loading-button v-on:clicked="addNewKeyspace" text="SAVE" className="btn" :loading="loadSpinner"></loading-button>
            </div>
        </div>

        <div class="keyspace-list">
            <div class="keyspace-item" v-for="ks in allKeyspaces" :key="ks">
                <div class="keyspace-label">
                    {{ks}}
                </div>
                <div class="right-side" @click="deleteKeyspace(ks)" >
                    <vue-icon icon="trash" className="vue-icon delete-icon" iconSize="14"></vue-icon>
                </div>
            </div>
        </div>

    </div>
</template>
<style scoped>

    .keyspace-handler-panel {
        display: flex;
        flex-direction: column;
        padding: var(--container-padding);
        width: 100%;
    }

    .keyspace-item {
        margin: var(--element-margin);
        padding: var(--container-padding);
        background-color: var(--gray-3);
        display: flex;
        align-items: center;
        justify-content: space-between;
        height: 22px;
        border: var(--container-light-border)
    }

    .new-keyspace-container {
        width: 500px;
        border: var(--container-darkest-border);
        position: absolute;
        background-color: var(--gray-2);
        top: 35%;
        right: 18.8%;
    }

    .new-keyspace-header {
        height: 22px;
        background-color: var(--gray-1);
        display: flex;
        justify-content: center;
        align-items: center;
        border-bottom: var(--container-darkest-border);
    }

    .close-container {
        position: absolute;
        right: 2px;
    }

    .panel-content {
         display: flex;
         align-items: center;
         padding: 5px 10px 0px 10px;
     }

    .keyspace-name-input {
        width: 100%;
    }

    .label {
        margin-right: 10px;
    }

    .row {
        display: flex;
        padding: 5px 5px 5px 10px;
        justify-content: flex-end;
    }

</style>



<script>
  import { mapGetters } from 'vuex';

  export default {
    name: 'KeyspaceHandler',
    data() {
      return {
        showNewKeyspacePanel: false,
        keyspaceName: '',
        loadSpinner: false,
      };
    },
    computed: {
      ...mapGetters(['allKeyspaces']),
    },
    methods: {
      addNewKeyspace() {
        if (!this.keyspaceName.length) return;
        this.loadSpinner = true;
        this.$store.dispatch('createKeyspace', this.keyspaceName)
          .then(() => { this.$notifyInfo(`New keyspace, ${this.keyspaceName}, successfully created!`); })
          .catch((error) => { this.$notifyError(error, 'Create keyspace'); })
          .then(() => { this.loadSpinner = false; this.keyspaceName = ''; this.showNewKeyspacePanel = false; });
      },
      deleteKeyspace(keyspace) {
        this.$store.dispatch('deleteKeyspace', keyspace)
          .then(() => { this.$notifyInfo(`Keyspace, ${keyspace}, successfully deleted!`); })
          .catch((error) => { this.$notifyError(error, 'Delete keyspace'); });
      },
    },
  };
</script>
