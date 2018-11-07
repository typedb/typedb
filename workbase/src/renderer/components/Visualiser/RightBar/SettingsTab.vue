<template>
    <div class="settings-tab-content">
        <div class="tab-header noselect">Settings</div>
        <query-settings-panel></query-settings-panel>
        <concept-display-panel :tabId="tabId"></concept-display-panel>
        <connection-settings-panel></connection-settings-panel>
    </div>
</template>
<script>
  import QuerySettingsPanel from './SettingsTab/QuerySettingsPanel.vue';
  import ConceptDisplayPanel from './SettingsTab/DisplaySettingsPanel';
  import ConnectionSettingsPanel from './SettingsTab/ConnectionSettingsPanel';

  export default {
    name: 'SettingsTab',
    props: ['tabId'],
    components: { QuerySettingsPanel, ConceptDisplayPanel, ConnectionSettingsPanel },
  };
</script>

<style scoped>
    .settings-tab-content {
        width: 214.14px;
    }
</style>
