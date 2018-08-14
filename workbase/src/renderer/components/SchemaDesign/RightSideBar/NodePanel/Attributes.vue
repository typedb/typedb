<template>
<div class="wrapper">
  <edit-bar 
  :localStore="localStore" 
  type="attribute"
  :typesAlreadySelected="attrTypes"
  :relatesToolTip="false"
  ></edit-bar>
    <div class="attributes-list">
      <div v-for="res in attrTypes" :key="res.label" class="attribute-elem" :class="{'editableLine':editingMode}">
        <div class="label">{{res.label}}</div>
        <div class="label datatype">{{res.dataType.split(".").pop()}}</div>
        <div class="label" v-if="editingMode&&res.editable"><div class="remove-btn" @click="deleteAttribute(res.label)">x</div></div>
        <div class="label" v-if="editingMode&&!res.editable"><div class="info-btn noselect">i</div></div>
      </div>
    </div>
</div>
</template>
<style scoped>
.remove-btn{
  background-color: #3E3E3F;
  outline: none;
  box-shadow: none;
  padding: 3px;
  border-radius: 2px;
  cursor: pointer;
  transition: all 0.1s ease-in-out;
  opacity: 0.7;
}

.info-btn{
  background-color: #3E3E3F;
  outline: none;
  box-shadow: none;
  padding: 2px 4px;
  border-radius: 10px;
  cursor: default;
  transition: all 0.1s ease-in-out;
  opacity: 0.7;
}

.remove-btn:hover{
  opacity: 1;
}
.attribute-elem.editableLine:hover{
  opacity: 1;
}

.attribute-elem.editableLine{
  opacity: 0.8;
}

.wrapper{
  padding: 5px;
}

.datatype{
  margin-left: auto;
}

.attributes-list{
  max-height: 200px;
  overflow: scroll;
}

.attribute-elem{
  margin-bottom:5px;
  display: flex;
  flex-direction: row;
  align-items: center;
  justify-content: space-between;
}

.margin-right{
  margin-right:5px;
}

.label{
  display: flex;
  flex-direction: row;
  padding:5px;
}
</style>
<script>
import EditBar from './Edit/EditBar.vue';

export default {
  name: 'NodeAttributesList',
  props: ['localStore'],
  components: { EditBar },
  data() {
    return {
      isEditMode: false,
      attrTypes: [],
    };
  },
  computed: {
    readyToRegisterEvents() {
      return this.localStore.isInit;
    },
    selectedNode() { return this.localStore.getSelectedNode(); },
    editingMode() {
      return this.localStore.getEditingMode();
    },
  },
  watch: {
    async selectedNode(selectedNode) {
      if (!selectedNode) { this.attrTypes = []; return; }
      const node = await this.localStore.getNode(selectedNode.id);
      const types = await node.attributes();
      this.attrTypes = await Promise.all(types.map(async t => ({ label: await t.getLabel(), dataType: await t.getDataType() })));
      this.attrTypes.sort((a, b) => ((a.label > b.label) ? 1 : -1));
    },
    readyToRegisterEvents() {
      this.localStore.registerCanvasEventHandler('click', () => { this.isEditMode = false; });
    },
  },
  methods: {
    deleteAttribute(attributeName) {
      this.localStore.dispatch('deleteAttribute', { label: this.selectedNode.label, attributeName })
        .then(() => { this.$notifySuccess(`Attribute [${attributeName}] removed.`); })
        .catch((err) => { this.$notifyError(err); });
    },
  },
};
</script>
