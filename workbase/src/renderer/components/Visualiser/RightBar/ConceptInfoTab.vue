<template>
    <div class="node-tab-content">
        <div class="tab-header">Concept Info</div>
        <concept-info-panel :localStore="localStore"></concept-info-panel>
        <attributes-panel :localStore="localStore"></attributes-panel>
    </div>
</template>

<script>

  import ConceptInfoPanel from './ConceptInfoTab/IdentityPanel';
  import AttributesPanel from './ConceptInfoTab/AttributesPanel';
  import ConceptDisplayPanel from './SettingsTab/DisplaySettingsPanel';

  export default {
    name: 'NodeTab',
    props: ['localStore'],
    components: { ConceptInfoPanel, AttributesPanel, ConceptDisplayPanel },
  };
</script>

<style scoped>



</style>
