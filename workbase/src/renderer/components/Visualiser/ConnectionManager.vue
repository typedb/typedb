<template>
    <div class="panel-container noselect">
        <div v-show="showConnectionSettings">
            <div class="panel-content">
                <div class="panel-content-item">
                    <h1 class="panel-label">Host:</h1>
                    <input class="input-small panel-value" v-model="serverHost">
                </div>
                <div class="panel-content-item">
                    <h1 class="panel-label">Port:</h1>
                    <input class="input-small panel-value" type="number" v-model="serverPort">
                </div>
            </div>
        </div>
    </div>
</template>

<script>
  import Settings from '../ServerSettings';

  export default {

    name: 'ConnectionManager',
    data() {
      return {
        showConnectionSettings: true,
        serverHost: Settings.getServerHost(),
        serverPort: Settings.getServerPort(),
      };
    },
    mounted() {
      this.$nextTick(() => {
        this.serverHost = Settings.getServerHost();
        this.serverPort = Settings.getServerPort();
      });
    },
    watch: {
      serverHost(newVal) {
        Settings.setServerHost(newVal);
      },
      serverPort(newVal) {
        Settings.setServerPort(newVal);
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
        justify-content: center;
    }

    .panel-label {
        width: 40px;
    }

    .panel-value {
        width: 100px;
        justify-content: center;
        display: flex;
    }

</style>
