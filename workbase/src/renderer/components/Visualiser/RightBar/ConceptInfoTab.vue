<template>
    <div class="node-tab-content">
        <div class="tab-header noselect">Concept Info</div>
        <identity-panel :tabId="tabId"></identity-panel>
        <attributes-panel :tabId="tabId"></attributes-panel>
    </div>
</template>
<script>
  import IdentityPanel from './ConceptInfoTab/IdentityPanel';
  import AttributesPanel from './ConceptInfoTab/AttributesPanel';

  export default {
    name: 'ConceptInfoTab',
    props: ['tabId'],
    components: { IdentityPanel, AttributesPanel },
  };
</script>
<style scoped>
    .node-tab-content {
        width: 213.14px;
    }
</style>
