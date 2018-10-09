<template>
    <div class="panel-container noselect">
        <div @click="toggleContent" class="panel-header">
            <vue-icon :icon="(showConnectionSettings) ?  'chevron-down' : 'chevron-right'" iconSize="14" className="vue-icon"></vue-icon>
            <h1>Connection Settings</h1>
        </div>
        <div v-show="showConnectionSettings">
            <div class="panel-content">
                <div class="panel-content-item">
                    <h1 class="panel-label">Host:</h1>
                    <input class="input-small panel-value" type="number" v-model="engineHost">
                </div>
                <div class="panel-content-item">
                    <h1 class="panel-label">Port:</h1>
                    <input class="input-small panel-value" type="number" v-model="engineGrpcPort">
                </div>
            </div>
        </div>
    </div>
</template>

<script>
  import Settings from '../../../EngineSettings';

  export default {

    name: 'ConnectionSettings',
    data() {
      return {
        showConnectionSettings: true,
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
    watch: {
      engineHost(newVal) {
        Settings.setEngineHost(newVal);
      },
      engineGrpcPort(newVal) {
        Settings.setEngineGrpcPort(newVal);
      },
    },
    methods: {
      toggleContent() {
        this.showConnectionSettings = !this.showConnectionSettings;
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
