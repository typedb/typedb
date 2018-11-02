<template>
    <div class="settings-tab-content">
        <div class="tab-header noselect">Settings</div>
        <query-settings-panel></query-settings-panel>
        <concept-display-panel :tabId="tabId"></concept-display-panel>
    </div>
</template>
<script>
  import QuerySettingsPanel from './SettingsTab/QuerySettingsPanel.vue';
  import ConceptDisplayPanel from './SettingsTab/DisplaySettingsPanel';

  export default {
    name: 'SettingsTab',
    props: ['tabId'],
    components: { QuerySettingsPanel, ConceptDisplayPanel },
  };
</script>

<style scoped>
    .settings-tab-content {
        width: 213.14px;
    }
</style>
