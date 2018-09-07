<template>
    <div class="settings-tab-content">
        <div class="tab-header">SETTINGS</div>
        <query-settings :localStore="localStore"></query-settings>
        <concept-display-panel :localStore="localStore"></concept-display-panel>
    </div>
</template>

<script>

  import QuerySettings from './QuerySettings.vue';
  import ConceptDisplayPanel from './ConceptDisplayPanel';

  export default {
    name: 'SettingsTab',
    props: ['localStore'],
    components: { QuerySettings, ConceptDisplayPanel },
  };
</script>

<style scoped>


</style>
