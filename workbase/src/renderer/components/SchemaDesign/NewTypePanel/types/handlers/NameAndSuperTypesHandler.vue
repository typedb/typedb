<template>
<div class="first-row">
      <div class="field flex-1">
        <span class="flex-1 small">Type Name</span> 
        <input class="grakn-input flex-1" v-model="typeLabel">
      </div>
      <div class="card-panel-list noselect flex-1">
        <div @click="showTypesList=!showTypesList" class="panel-title">
          <span class="flex-2 small">Subtype of</span>
          <span class="flex-3 align-center select-super break-all">{{superType}} <caret-icon :toggleNorth="showTypesList"></caret-icon></span>
          <!-- List of selected types -->
          <div v-show="showTypesList" class="card-panel-list inner-card">
            <span class="select-span"><input class="grakn-input" v-model="typeFilter" placeholder="filter" ref="filterinput"></span>
            <div class="existing-types-list card">
              <ul>
                <li v-for="concept in filteredTypes" :key="concept" @click="superTypeSelected(concept)" class="break-all">{{concept}}</li>
              </ul>
            </div>
          </div>
        </div>
      </div>
</div>
</template>
<script>
import CaretIcon from '@/components/UIElements/CaretIcon.vue';

export default {
  name: 'SuperTypesHandler',
  props: ['instances', 'conceptType'],
  components: { CaretIcon },
  data() {
    return {
      typeFilter: '',
      typeLabel: '',
      showTypesList: false,
      superType: this.conceptType,
    };
  },
  watch: {
    showTypesList(val) {
      if (val) this.$nextTick(() => { this.$refs.filterinput.focus(); });
    },
  },
  computed: {
    filteredTypes: function filter() {
      if (this.instances === undefined) return [];
      return this.instances
        .filter(type => String(type).toLowerCase().indexOf(this.typeFilter) > -1);
    },
  },
  created() {
    this.$on('clear-panel', this.clearPanel);
  },
  mounted() {
    this.$nextTick(() => {
    });
  },
  methods: {
    getTypeLabel() {
      return this.typeLabel;
    },
    getSuperType() {
      return this.superType;
    },
    clearPanel() {
      this.superType = this.conceptType;
      this.typeLabel = '';
    },
    superTypeSelected(type) {
      this.superType = type;
      this.$emit('supertype-selected', type);
    },
  },
};
</script>
<style scoped>

* {
  color:white;
}
.inner-card{
  margin-top:8px;
  position: absolute;
  top: 100%;
  z-index: 10;
  background-color: #262626;
  right: -5%;
}


li:hover{
  background-color: #404040;
}

.select-super{
  cursor: pointer;
  background-color: #2E2D2D;
}

.break-all{
    word-break: break-all;
}

.panel-title{
  display: flex;
  flex-direction: column;
  position: relative;
}

.select-span{
  display: flex;
  justify-content: flex-end;
  padding: 0px 5px;
}

.existing-types-list{
  max-height: 105px;
  overflow: scroll;
  margin-top:5px;
  margin-bottom:5px;
  padding:5px;
}


li{
  cursor: pointer;
  margin: 2px;
}


.card-panel-list {
    padding: 2px;
    margin-bottom: 10px;
    display: flex;
    flex-direction: column;
}

.field {
    padding: 5px;
    margin-bottom: 10px;
    display: flex;
    flex-direction: column;
}

.first-row{
  display: flex;
  flex-direction: row;
}

.small{
  font-size: 80%;
  padding: 2px;
}

</style>
