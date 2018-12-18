<template>
    <div class="settings-tab-content">
        <div class="tab-header noselect">Settings</div>
        <concept-display-panel></concept-display-panel>
    </div>
</template>
<script>
  import ConceptDisplayPanel from './SettingsTab/DisplaySettingsPanel';

  export default {
    name: 'SettingsTab',
    components: { ConceptDisplayPanel },
  };
</script>

<style scoped>
    .settings-tab-content {
        width: 214.14px;
    }
</style>
