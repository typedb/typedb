<template>
    <!-- List of selected types -->
    <div class="existing-list" v-if="filteredTypes.length">
        <span class="select-span"><input class="grakn-input" v-model="typeFilter" placeholder="filter" ref="filterinput"></span>
        <div class="existing-types-list card">
            <ul>
                <li v-for="concept in filteredTypes" :key="concept" @click="selectType(concept)" class="break-all">{{concept}}</li>
            </ul>
        </div>
    </div>
    <div v-else class="inner-card">There are no types to be added.</div>
</template>

<style scoped>
.existing-list{
  margin-top:4px;
  background-color: #262626;
}

li:hover{
  background-color: #404040;
}

.break-all{
    word-break: break-all;
}

li{
  cursor: pointer;
  margin: 4px;
}
</style>
<script>

import { ADD_TYPE } from '@/components/shared/StoresActions';

export default {
  name: 'AddToExistingList',
  props: ['localStore', 'typesAlreadySelected', 'type'],
  data() {
    return {
      typeFilter: '',
    };
  },
  computed: {
    instances() {
      const meta = this.$store.getters.metaTypeInstances;
      const metaTypeTypes = [(this.type === 'attribute') ? 'attributes' : 'roles'];

      return (Object.keys(meta).length > 0) ? meta[metaTypeTypes] : [];
    },
    filteredTypes: function filter() {
      if (this.instances === undefined) return [];
      return this.instances
        .filter(type => String(type).toLowerCase().indexOf(this.typeFilter) > -1)
        .filter(type => !(this.typesAlreadySelected.map(x => x.label).includes(type)))
        .sort();
    },
    readyToRegisterEvents() {
      return this.localStore.isInit;
    },
  },
  watch: {
    readyToRegisterEvents() {
      this.localStore.registerCanvasEventHandler('click', () => { this.$emit('close-tool-tip'); });
    },
  },
  methods: {
    selectType(typeLabel) {
      this.localStore.dispatch(ADD_TYPE, { type: this.type, typeLabel })
        .then(() => {
          this.$notifySuccess(`[${typeLabel}] added.`);
          this.localStore.setEditingMode(undefined);
        })
        .catch((err) => { this.$notifyError(err); });
    },
  },
};
</script>

