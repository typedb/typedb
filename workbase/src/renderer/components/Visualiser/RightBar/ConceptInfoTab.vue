<template>
    <div class="node-tab-content">
        <div class="tab-header noselect">Concept Info</div>
        <identity-panel :localStore="localStore"></identity-panel>
        <attributes-panel :localStore="localStore"></attributes-panel>
        <relationships-panel :localStore="localStore"></relationships-panel>
    </div>
</template>

<script>

  import IdentityPanel from './ConceptInfoTab/IdentityPanel';
  import AttributesPanel from './ConceptInfoTab/AttributesPanel';
  import RelationshipsPanel from './ConceptInfoTab/RelationshipsPanel';


  export default {
    name: 'ConceptInfoTab',
    props: ['localStore'],
    components: { IdentityPanel, AttributesPanel, RelationshipsPanel },
  };
</script>

<style scoped>

    .node-tab-content {
        width: 213.14px;
    }

</style>
