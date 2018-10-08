<template>
    <div class="settings-tab-content">
        <div class="tab-header noselect">Settings</div>
        <query-settings-panel></query-settings-panel>
        <concept-display-panel></concept-display-panel>
        <connection-settings-panel></connection-settings-panel>
    </div>
</template>
<script>
  import QuerySettingsPanel from './SettingsTab/QuerySettingsPanel.vue';
  import ConceptDisplayPanel from './SettingsTab/DisplaySettingsPanel';
  import ConnectionSettingsPanel from './SettingsTab/ConnectionSettingsPanel';

  export default {
    name: 'SettingsTab',
    components: { QuerySettingsPanel, ConceptDisplayPanel, ConnectionSettingsPanel },
  };
</script>

<style scoped>
    .settings-tab-content {
        width: 213.14px;
    }
</style>
