<template>
    <div class="node-tab-content">
        <div class="tab-header">Concept Info</div>
        <identity-panel :localStore="localStore"></identity-panel>
        <attributes-panel :localStore="localStore"></attributes-panel>
    </div>
</template>

<script>

  import IdentityPanel from './ConceptInfoTab/IdentityPanel';
  import AttributesPanel from './ConceptInfoTab/AttributesPanel';

  export default {
    name: 'ConceptInfoTab',
    props: ['localStore'],
    components: { IdentityPanel, AttributesPanel },
  };
</script>

<style scoped>

    .node-tab-content {
        width: 213.14px;
    }

</style>
