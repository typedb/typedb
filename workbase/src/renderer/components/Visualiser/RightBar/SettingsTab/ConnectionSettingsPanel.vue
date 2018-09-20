<template>
    <div class="panel-container noselect">
        <div @click="toggleContent" class="panel-header">
            <vue-icon :icon="(showConnectionSettings) ?  'chevron-down' : 'chevron-right'" iconSize="14"></vue-icon>
            <h1>Connection Settings</h1>
        </div>
        <div v-show="showConnectionSettings">
            <div class="panel-content" v-if="!currentKeyspace">
                Please select a keyspace
            </div>

            <div class="panel-content" v-else>
                <div class="panel-content-item">
                    <h1 class="panel-label">Host:</h1>
                    <div class="panel-value"><vue-input :defaultValue="engineHost" v-on:input-changed="updateEngineHost" className="vue-input vue-input-small"></vue-input></div>
                </div>
                <div class="panel-content-item">
                    <h1 class="panel-label">Port:</h1>
                    <div class="panel-value"><vue-input :defaultValue="engineGrpcPort" v-on:input-changed="updateEngineGrpcPort" className="vue-input vue-input-small"></vue-input></div>
                </div>
            </div>
        </div>
    </div>
</template>

<script>
  import Settings from '../../../EngineSettings';

  export default {

    name: 'ConnectionSettings',
    props: ['localStore'],
    data() {
      return {
        showConnectionSettings: false,
        engineHost: Settings.getEngineHost(),
        engineGrpcPort: Settings.getEngineGrpcPort(),
      };
    },
    mounted() {
      this.$nextTick(() => {
        this.engineHost = Settings.getEngineHost();
        this.engineGrpcPort = Settings.getEngineGrpcPort();
      });
    },
    computed: {
      currentKeyspace() {
        return this.localStore.getCurrentKeyspace();
      },
    },
    methods: {
      toggleContent() {
        this.showConnectionSettings = !this.showConnectionSettings;
      },
      updateEngineHost(newVal) {
        Settings.setEngineHost(newVal);
      },
      updateEngineGrpcPort(newVal) {
        Settings.setEngineGrpcPort(newVal);
      },
    },
  };
</script>

<style scoped>

    .panel-content {
        padding: var(--container-padding);
        border-bottom: var(--container-darkest-border);
        display: flex;
        flex-direction: column;
    }

    .panel-content-item {
        padding: var(--container-padding);
        display: flex;
        flex-direction: row;
        align-items: center;
        height: var(--line-height);
    }

    .panel-label {
        width: 90px;
    }

    .panel-value {
        width: 100px;
        justify-content: center;
        display: flex;
    }

</style>
