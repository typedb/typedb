<template>
    <div class="node-tab-content">
        <div class="tab-header noselect">Concept Info</div>
        <identity-panel :tabId="tabId"></identity-panel>
        <attributes-panel :tabId="tabId"></attributes-panel>
        <relationships-panel :localStore="localStore"></relationships-panel>
    </div>
</template>
<script>
  import IdentityPanel from './ConceptInfoTab/IdentityPanel';
  import AttributesPanel from './ConceptInfoTab/AttributesPanel';
  import RelationshipsPanel from './ConceptInfoTab/RelationshipsPanel';


  export default {
    name: 'ConceptInfoTab',
    props: ['tabId'],
    components: { IdentityPanel, AttributesPanel, RelationshipsPanel },
  };
</script>
<style scoped>
    .node-tab-content {
        width: 213.14px;
    }
</style>
