<template>
    <div class="node-tab-content">
        <div class="tab-header">CONCEPT INFO</div>
        <concept-info-panel :localStore="localStore"></concept-info-panel>
        <attributes-panel :localStore="localStore"></attributes-panel>
    </div>
</template>

<script>

  import ConceptInfoPanel from './ConceptInfoPanel';
  import AttributesPanel from './AttributesPanel';
  import ConceptDisplayPanel from './ConceptDisplayPanel';

  export default {
    name: 'NodeTab',
    props: ['localStore'],
    components: { ConceptInfoPanel, AttributesPanel, ConceptDisplayPanel },
  };
</script>

<style scoped>



</style>
