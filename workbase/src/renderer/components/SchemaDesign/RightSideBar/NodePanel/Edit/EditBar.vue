<template>
    <div class="edit-bar" v-show="showBar">
      <div v-show="editingMode === type" class="select-wrapper">
        <div class="add-btn noselect" @click="showEditToolTip = !showEditToolTip">Add {{metaType}}</div>
        <edit-tool-tip
          :relatesToolTip="relatesToolTip"
          :typesAlreadySelected="typesAlreadySelected"
          :showEditToolTip="showEditToolTip" 
          :type="type"
          v-on:hide-tool-tip="showEditToolTip=false"
>        </edit-tool-tip>
      </div>
      <span @click="toggleEditingMode" v-if="!(editingMode === type)" class="edit-icon noselect"><img src="static/img/icons/icon_edit_white.svg"></span>
      <span @click="toggleEditingMode" v-else class="close-edit noselect">x</span>
    </div>
</template>

<style scoped>
.add-btn{
  display: flex;
  font-size: 80%;
  padding: 2px;
  margin-right: auto;
  background-color: #3E3E3F;
  outline: none;
  box-shadow: none;
  padding: 3px;
  border-radius: 2px;
  cursor: pointer;
  transition: all 0.1s ease-in-out;
  opacity: 0.7;
}

.edit-bar{
    display: flex;
    margin-bottom: 8px;
}

.close-edit{
  padding-right: 4px;
  font-size: 105%;
  height: 15px;
  cursor: pointer;
  margin-left: auto;
  display: flex;
}

.edit-icon{
    margin-left: auto;
    display: flex;
    height: 15px;
    cursor: pointer;
}

.select-wrapper{
    display: flex;
    flex: 1;
    align-items: center;
    flex-flow: column;
}

</style>
<script>
import CaretIcon from '@/components/UIElements/CaretIcon.vue';
import EditToolTip from './EditToolTip';

export default {
  name: 'EditBar',
  props: ['typesAlreadySelected', 'relatesToolTip', 'type'],
  components: { CaretIcon, EditToolTip },
  data() {
    return {
      isEditMode: false,
      showTypesList: false,
      typeFilter: '',
      showEditToolTip: false,
    };
  },
  computed: {
    showBar() {
      return (this.$store.getters.selectedNode);
    },
    editingMode() {
      return this.$store.getters.editingMode;
    },
    metaType() {
      if (this.type === 'attribute') {
        return 'attribute';
      }
      return 'role';
    },
  },
  watch: {
    showBar(val) {
      if (!val) {
        this.$store.commit('editingMode', undefined);
        this.showEditToolTip = false;
      }
    },
  },
  methods: {
    toggleEditingMode() {
      if (this.editingMode === this.type) {
        this.$store.commit('editingMode', undefined);
        this.showEditToolTip = false;
      } else {
        this.$store.commit('editingMode', this.type);
        this.showEditToolTip = false;
      }
    },
  },
};
</script>

