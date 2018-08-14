<template>
    <div class="card-panel-list  noselect">       
      <div @click="showTypesPanel=!showTypesPanel" class="panel-title">
        <font>{{panelTitle}}</font>
        <plus-minus :isActive="showTypesPanel"></plus-minus>
      </div>
      <div class="card-panel-list inner-card" :class="{'open':showTypesPanel}">
          <span class="select-span">
            <input class="grakn-input" v-model="typeFilter" placeholder="type label" ref="filterinput">
             <select v-model="newTypeDatatype" v-if="metaType==='attribute'" class="flex-2" :disabled="!typeFilter.length">
                <option>string</option>
                <option>date</option>
                <option>boolean</option>
                <option>long</option>
              <option>double</option>
            </select>
            <span class="select-span" v-if="allowNewType">
              <div class="btn small" @click="addNewType" :class="{'disabled':!typeFilter.length}">Add<i class="fa fa-caret-down" aria-hidden="true"></i>
              </div>
            </span>
          </span>
          <div class="existing-types-list">
            <ul>
              <li v-for="conceptType in filteredTypes" :key="conceptType" @click="addExistingType(conceptType)">{{conceptType}}</li>
            </ul>
          </div>
        </div>
       <div class="list-selected">
          <div  class="selected-line" v-for="(concept,index) in typesToBeAdded" :key="index">{{concept.label}}<div class="remove-btn" @click="removeType(concept,index)">x</div></div>
        </div>
    </div>
</template>
<script>
import PlusMinus from '@/components/UIElements/PlusMinus.vue';

export default {
  name: 'TypesMultiselectHandler',
  props: ['instances', 'panelTitle', 'metaType', 'allowNewType'],
  components: { PlusMinus },
  data() {
    return {
      typesToBeAdded: [],
      typesAlreadySelected: [],
      showNewType: false,
      typeFilter: '',
      showTypesPanel: false,
      newTypeDatatype: 'string',
    };
  },

  computed: {
    filteredTypes: function filter() {
      if (this.instances === undefined) return [];
      return this.instances
        .filter(type => type !== this.metaType)
        .filter(type => String(type).toLowerCase().indexOf(this.typeFilter) > -1)
        .filter(type => !(this.typesAlreadySelected.includes(type)));
    },
  },
  created() {
    this.$on('clear-panel', this.clearPanel);
  },
  watch: {
    showTypesPanel(val) {
      if (val) this.$nextTick(() => { this.$refs.filterinput.focus(); });
    },
  },
  methods: {
    addExistingType(label) {
      this.typesToBeAdded.push({ label, new: false });
      this.typesAlreadySelected.push(label);
    },
    addNewType() {
      if (!this.typeFilter.length) return;
      const newType = { label: this.typeFilter, dataType: this.newTypeDatatype, new: true };
      this.typesToBeAdded.push(newType);
      this.typeFilter = '';
    },
    removeType(type, index) {
      this.typesToBeAdded.splice(index, 1);
      if (!type.new) {
        const arrIndex = this.typesAlreadySelected.indexOf(type.name);
        this.typesAlreadySelected.splice(arrIndex, 1);
      }
    },
    getTypes() {
      return this.typesToBeAdded;
    },
    clearPanel() {
      this.typesAlreadySelected = [];
      this.typesToBeAdded = [];
      this.typeFilter = '';
      this.showTypesPanel = false;
      this.showNewType = false;
    },
  },
};
</script>
<style scoped>

select{
  color:black;
}
.btn.disabled{
  opacity: 0.9;
  cursor: default;
}

.inner-card{
  margin-top:8px;
  overflow-y: scroll;
  transition-property: all;
  transition-duration: .5s;
  max-height: 0;
}

.inner-card.open{
  max-height: 200px;
}

.panel-title{
   display:flex; 
   align-items:center; 
   cursor:pointer;
   background-color: #2E2D2D;
   justify-content: space-between;
   padding: 4px;
}

.list-selected{
  margin-top: 5px;
}

li:hover{
  background-color: #404040;
}
  
.new-type-line{
  padding:5px;
  margin-top:5px;
  display: flex;
  flex-direction: row;
}

.select-span{
  display: flex;
  justify-content: flex-end;
  padding: 0px 5px;
  background-color: #2E2D2D;
}

.existing-types-list{
  height: 60px;
  overflow: scroll;
  margin-top:5px;
  margin-bottom:5px;
  padding:5px;
}

.selected-line{
  display: flex;
  justify-content: space-between;
  padding: 2px 5px;
  opacity: 0.8;
}

li{
  cursor: pointer;
  margin: 2px;
}
  
.add-line{
  display: inline-flex;
  justify-content: space-around;
  margin-top:5px;
}

.card-panel-list {
    margin-bottom: 10px;
    display: flex;
    flex-direction: column;
}

.small{
  height:18px;
}

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
.selected-line:hover{
  opacity: 1;
}

</style>
