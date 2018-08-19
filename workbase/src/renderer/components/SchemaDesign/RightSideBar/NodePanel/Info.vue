<template>
<div class="node-panel-wrapper" v-show="node">
    <div class="field-line">label: {{node.label}}</div>
    <div class="field-line">id: {{node.id}}</div>
    <div class="field-line">baseType: {{ node.baseType}}</div>
</div>
</template>
<style>
.slider.open {
	max-height: 400px;

}
.node-panel-wrapper{
  padding:8px;
}
.btn.delete{
  font-weight: bold;
  margin: 5px 10px;
}

.field-line{
  margin-bottom: 5px;
}
</style>
<script>
export default {
  name: 'NodePanel',
  props: ['localStore'],
  computed: {
    node() {
      const node = this.localStore.getSelectedNode();
      return (node) || {};
    },
  },
};
</script>
