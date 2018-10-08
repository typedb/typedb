<template>
  <div class="list">
    <name-and-super-types-handler :instances="instances['relationships']" conceptType="relationship" ref="superTypesHandler"></name-and-super-types-handler>
   </div>
</template>
<script>
import { DEFINE_RELATIONSHIP_TYPE } from '@/components/shared/StoresActions';
import NameAndSuperTypesHandler from './handlers/NameAndSuperTypesHandler.vue';

export default {
  name: 'RelationshipTab',
  props: ['instances'],
  components: { NameAndSuperTypesHandler },
  created() {
    this.$on('clear-panel', this.clearPanel);
  },
  methods: {
    insertType() {
      const label = this.$refs.superTypesHandler.getTypeLabel();
      const superType = this.$refs.superTypesHandler.getSuperType();

      return this.$store.dispatch(DEFINE_RELATIONSHIP_TYPE, { label, superType });
    },
    clearPanel() {
      this.$refs.superTypesHandler.$emit('clear-panel');
    },
  },
};
</script>
<style scoped>
.list {
    display: flex;
    flex-direction: column;
}
</style>
