<template>
<div class="wrapper">
  <edit-bar
    :localStore="localStore"
    :typesAlreadySelected="playsTypes"
    :relatesToolTip="false"
    type="plays"
  ></edit-bar>
    <div class="attributes-list">
      <div v-for="res in playsTypes" :key="res.label" class="attribute-elem" :class="{'editableLine':editingMode}">
        <div class="label">{{res.label}}</div>
        <div class="label" v-show="editingMode"><div class="remove-btn" @click="deletePlays(res.label)">x</div></div>
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
  name: 'NodePlaysList',
  props: ['localStore'],
  components: { EditBar },
  data() {
    return {
      isEditMode: false,
      playsTypes: [],
    };
  },
  created() {
    this.localStore.registerCanvasEventHandler('click', () => { this.isEditMode = false; });
  },
  computed: {
    selectedNode() {
      return this.localStore.getSelectedNode();
    },
    editingMode() {
      return this.localStore.getEditingMode();
    },
  },
  watch: {
    async selectedNode(selectedNode) {
      if (!selectedNode) { this.playsTypes = []; return; }
      const node = await this.localStore.getNode(selectedNode.id);
      const types = await node.plays();
      const explicitTypes = await Promise.all(types.map(async t => (!(await t.isImplicit()) ? t : null))).then(types => types.filter(t => t));
      this.playsTypes = await Promise.all(explicitTypes.map(async t => ({ label: await t.getLabel() })));
      this.playsTypes.sort((a, b) => ((a.label > b.label) ? 1 : -1));
    },
  },
  methods: {
    deletePlays(roleName) {
      this.localStore.dispatch('deletePlaysRole', { label: this.selectedNode.label, roleName })
        .then(() => { this.$notifySuccess(`Role [${roleName}] removed.`); })
        .catch((err) => { this.$notifyError(err); });
    },
  },
};
</script>
