<template>
  <div class="list">
    <name-and-super-types-handler :instances="instances['attributes']" conceptType="attribute" ref="superTypesHandler"></name-and-super-types-handler>
    <datatype-handler :enableDataType="enableDataTypeInAttribute" ref="dataTypeHandler"></datatype-handler>
  </div>
</template>
<script>
import { DEFINE_ATTRIBUTE_TYPE } from '@/components/shared/StoresActions';
import NameAndSuperTypesHandler from './handlers/NameAndSuperTypesHandler.vue';
import DatatypeHandler from './handlers/DatatypeHandler.vue';

export default {
  name: 'AttributeTab',
  props: ['instances', 'localStore'],
  components: { DatatypeHandler, NameAndSuperTypesHandler },
  data() {
    return {
      enableDataTypeInAttribute: true,
    };
  },
  created() {
    this.$on('clear-panel', this.clearPanel);
  },
  mounted() {
    this.$nextTick(() => {
      // If the current attribute is subtype of another existing one,
      // we cannot set the datatype, and it will be inherited from the supertype.
      this.$refs.superTypesHandler.$on('supertype-selected', (type) => {
        this.enableDataTypeInAttribute = type === 'attribute';
      });
    });
  },
  methods: {
    insertType() {
      const label = this.$refs.superTypesHandler.getTypeLabel();
      const superType = this.$refs.superTypesHandler.getSuperType();
      const dataType = this.$refs.dataTypeHandler.getDataType();
      const inheritDatatype = !this.enableDataTypeInAttribute;

      return this.localStore.dispatch(DEFINE_ATTRIBUTE_TYPE, { label, superType, dataType, inheritDatatype });
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
