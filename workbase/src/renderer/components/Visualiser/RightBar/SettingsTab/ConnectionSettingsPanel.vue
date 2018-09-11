<template>
    <div class="panel-container">
        <div @click="toggleContent" class="panel-header">
            <vue-icon :icon="(showConnectionSettings) ?  'chevron-down' : 'chevron-right'" iconSize="14"></vue-icon>
            <h1>Connection Settings</h1>
        </div>
        <div class="content" v-show="showConnectionSettings">
            <div class="content-item">
                <h1 class="label">Host</h1>
                <div class="value"><vue-input :defaultValue="engineHost" v-on:input-changed="updateEngineHost" className="vue-input vue-input-small"></vue-input></div>
            </div>
            <div class="content-item">
                <h1 class="label">Port</h1>
                <div class="value"><vue-input :defaultValue="engineGrpcPort" v-on:input-changed="updateEngineGrpcPort" className="vue-input vue-input-small"></vue-input></div>
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

    .content {
        padding: var(--container-padding);
        border-bottom: var(--container-darkest-border);
        display: flex;
        flex-direction: column;
        max-height: 120px;
    }

    .content-item {
        padding: var(--container-padding);
        display: flex;
        flex-direction: row;
        align-items: center;
        height: var(--line-height);
    }

    .label {
        width: 90px;
    }

    .value {
        width: 100px;
        justify-content: center;
        display: flex;
        position: absolute;
        right: 10px;
    }

</style>
