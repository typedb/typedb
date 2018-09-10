<template>
    <div class="settings-tab-content">
        <div class="tab-header">Settings</div>
        <query-settings :localStore="localStore"></query-settings>
        <concept-display-panel :localStore="localStore"></concept-display-panel>
        <connection-settings :localStore="localStore"></connection-settings>

    </div>
</template>

<script>

  import QuerySettings from './SettingsTab/QuerySettingsPanel.vue';
  import ConceptDisplayPanel from './SettingsTab/DisplaySettingsPanel';
  import ConnectionSettings from './SettingsTab/ConnectionSettingsPanel';

  export default {
    name: 'SettingsTab',
    props: ['localStore'],
    components: { QuerySettings, ConceptDisplayPanel, ConnectionSettings },
  };
</script>

<style scoped>


</style>
