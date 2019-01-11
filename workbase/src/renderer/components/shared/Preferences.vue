<template>
    <div class="preferences-container z-depth-5">
        <div class="preferences-header">
            <div class="preferences-title">Preferences</div>
            <div class="close-container" @click="$emit('close-preferences')"><vue-icon icon="cross" iconSize="12" className="tab-icon"></vue-icon></div>
        </div>
        <div class="preferences-content">


            <div class="connection-container">
                <div class="container-title">
                    Connection
                </div>
                <div class="connection-content">
                    <h1 class="connection-label">Host:</h1>
                    <input class="input" v-model="serverHost">
                    <h1 class="connection-label port">Port:</h1>
                    <input class="input" type="number" v-model="serverPort">
                    <loading-button v-on:clicked="testConnection" :text="connectionTest" className="btn test-btn" :loading="connectionTest === 'testing'"></loading-button>
                    <!-- <button @click="testConnection" :class="connectionTest" class="btn test-btn" :disabled="(connectionTest !== 'Test')? true : false">{{connectionTest}}</button> -->
                </div>
            </div>

            <div class="keyspaces-container">
                <div class="container-title">
                    Keyspaces
                </div>
                <div class="keyspaces-content">
                    
                    <div class="keyspaces-list">
                        <div class="keyspace-item" :class="(index % 2) ? 'even' : 'odd'" v-for="(ks,index) in allKeyspaces" :key="index">
                            <div class="keyspace-label">
                                {{ks}}
                            </div>
                            <div class="right-side" @click="deleteKeyspace(ks)" >
                                <vue-icon icon="trash" className="vue-icon delete-icon" iconSize="14"></vue-icon>
                            </div>
                        </div>
                    </div>
                    <div class="new-keyspace">
                        <input class="input keyspace-input" v-model="keyspaceName" placeholder="Keyspace name">
                        <loading-button v-on:clicked="addNewKeyspace" text="Create New Keyspace" className="btn new-keyspace-btn" :loading="loadSpinner"></loading-button>
                    </div>
                </div>
            </div>


            <div class="logout-container" v-if="userLogged">
                <div class="keyspaces-content">
                    <button class="btn" @click="logout">Logout</button>

                </div>
            </div>

        </div>
    </div>
</template>
<style scoped>
    .odd {
        background-color:var(--gray-1);
    }

    .keyspace-input {
        width: 100%;
    }

    .new-keyspace {
        display: flex;
        align-items: center;
        width: 100%;
        padding-top: var(--container-padding);
    }

    .keyspaces-list {
        display: flex;
        flex-direction: column;
        width: 100%;
        border: var(--container-darkest-border);
        max-height: 400px;
        overflow: auto;
    }

    .keyspaces-list::-webkit-scrollbar {
        width: 2px;
    }

    .keyspaces-list::-webkit-scrollbar-thumb {
        background: var(--green-4);
    }

    .keyspace-item {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: var(--container-padding);
        min-height: 30px;
    }

    .keyspaces-content {
        padding: var(--container-padding);
        display: flex;
        flex-direction: column;
    }

    .preferences-container {
        position: absolute;
        width: 400px;
        background-color: var(--gray-2);
        top: 10%;
        right: 36%;
        z-index: 2;
        border: var(--container-darkest-border);
        display: flex;
        flex-direction: column;
    }

    .preferences-header {
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

    .preferences-content {
        display: flex;
        flex-direction: column;
        width: 100%;
    }

    .connection-container {
        padding: var(--container-padding);
        width: 100%;
        border-bottom: var(--container-darkest-border);
    }

    .keyspaces-container {
        padding: var(--container-padding);
        width: 100%;
        border-bottom: var(--container-darkest-border);
    }

    .logout-container {
        padding: var(--container-padding);
        width: 100%;
    }

    .container-title {
        padding: var(--container-padding);
    }

    .connection-content {
        padding: var(--container-padding);
        display: flex;
        align-items: center;
        justify-content: space-between;
    }

    .test-btn {
        margin: 0px;
        width: 50px;
    }

    .Valid {
        color: var(--green-4);
    }

    .Invalid {
        color: var(--red-4);
    }

</style>

<script>
import { mapGetters } from 'vuex';
import Settings from '../ServerSettings';


export default {
  name: 'Preferences',
  data() {
    return {
      serverHost: Settings.getServerHost(),
      serverPort: Settings.getServerPort(),
      connectionTest: '',
      keyspaceName: '',
      loadSpinner: false,
    };
  },
  mounted() {
    this.$nextTick(() => {
      this.serverHost = Settings.getServerHost();
      this.serverPort = Settings.getServerPort();
    });
  },
  created() {
    this.connectionTest = (this.isGraknRunning) ? 'Valid' : 'Invalid';
  },
  computed: {
    ...mapGetters(['isGraknRunning', 'allKeyspaces', 'userLogged']),
  },
  watch: {
    serverHost(newVal) {
      this.connectionTest = 'Test';
      Settings.setServerHost(newVal);
    },
    serverPort(newVal) {
      this.connectionTest = 'Test';
      Settings.setServerPort(newVal);
    },
    isGraknRunning(newVal) {
      this.connectionTest = (newVal) ? 'Valid' : 'Invalid';
    },
  },
  methods: {
    testConnection() {
      this.connectionTest = 'testing';
      this.$store.dispatch('initGrakn');
    },
    addNewKeyspace() {
      if (!this.keyspaceName.length) return;
      this.loadSpinner = true;
      this.$store.dispatch('createKeyspace', this.keyspaceName)
        .then(() => { this.$notifyInfo(`New keyspace, ${this.keyspaceName}, successfully created!`); })
        .catch((error) => { this.$notifyError(error, 'Create keyspace'); })
        .then(() => { this.loadSpinner = false; this.keyspaceName = ''; this.showNewKeyspacePanel = false; });
    },
    deleteKeyspace(keyspace) {
      this.$notifyConfirmDelete(`Are you sure you want to delete ${keyspace} keyspace?`,
        () => this.$store.dispatch('deleteKeyspace', keyspace)
          .then(() => this.$notifyInfo(`Keyspace, ${keyspace}, successfully deleted!`))
          .catch((error) => { this.$notifyError(error, 'Delete keyspace'); }));
    },
    async logout() {
      await this.$store.dispatch('logout');
      this.$router.push('/login');
    },
  },
};
</script>
