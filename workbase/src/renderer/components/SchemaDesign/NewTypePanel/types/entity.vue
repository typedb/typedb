<template>
  <div class="list">
    <name-and-super-types-handler :instances="instances['entities']" conceptType="entity" ref="superTypesHandler"></name-and-super-types-handler>
  </div>
</template>
<script>
import { DEFINE_ENTITY_TYPE } from '@/components/shared/StoresActions';
import NameAndSuperTypesHandler from './handlers/NameAndSuperTypesHandler.vue';

export default {
  name: 'EntityTab',
  props: ['instances', 'localStore'],
  components: { NameAndSuperTypesHandler },
  data() {
    return {
      showEntitiesList: false,
    };
  },
  created() {
    this.$on('clear-panel', this.clearPanel);
  },
  mounted() {
    this.$nextTick(() => {
    });
  },
  methods: {
    insertType() {
      const label = this.$refs.superTypesHandler.getTypeLabel();
      const superType = this.$refs.superTypesHandler.getSuperType();

      return this.localStore.dispatch(DEFINE_ENTITY_TYPE, { label, superType });
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
