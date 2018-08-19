<template>
  <div class="list">
    <name-and-super-types-handler :instances="this.metaTypeInstances['roles']" conceptType="role" ref="superTypesHandler"></name-and-super-types-handler>
    <div class="add-line">
      <loading-button 
        :clickFunction="insertType" 
        value="Add" 
        :isLoading="isLoading">
      </loading-button>
    </div>
  </div>
</template>
<script>
import { DEFINE_ROLE } from '@/components/shared/StoresActions';
import NameAndSuperTypesHandler from '../../../NewTypePanel/types/handlers/NameAndSuperTypesHandler.vue';

export default {
  name: 'RoleTab',
  props: ['localStore'],
  components: { NameAndSuperTypesHandler },
  data() {
    return {
      showEntitiesList: false,
      isLoading: false,
    };
  },
  computed: {
    metaTypeInstances() { return this.localStore.getMetaTypeInstances(); },
  },
  created() {
    this.$on('clear-panel', this.clearPanel);
  },
  methods: {
    insertType() {
      const label = this.$refs.superTypesHandler.getTypeLabel();
      const superType = this.$refs.superTypesHandler.getSuperType();

      this.localStore.dispatch(DEFINE_ROLE, { label, superType })
        .then(() => {
          this.$refs.superTypesHandler.$emit('clear-panel');
          this.$notifySuccess('Concept successfully added!');
        })
        .catch((error) => { this.$notifyError(error); })
        .then(() => {
          this.isLoading = false;
          this.localStore.setEditingMode(undefined);
        });
    },
  },
};
</script>
<style scoped>
.list {
    display: flex;
    flex-direction: column;
}

.add-line{
  display: flex;
  flex-direction: row;
  justify-content: flex-end;
  align-items: center;
  height: 40px;
}
</style>
