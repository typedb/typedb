<template>
    <div class="card-panel-list noselect" v-show="enableDataType">
      <div @click="showTypesList=!showTypesList" style="display:flex; align-items:center; cursor:pointer;">
        <font style="color:white;">Datatype</font>
        <span style="margin-left:auto; color:white;">{{dataType}}</span>
        <caret-icon :toggleNorth="showTypesList"></caret-icon>
      </div>
        <div v-show="showTypesList" class="card-panel-list inner-card">
          <div class="existing-types-list">
            <ul>
              <li v-for="type in dataTypes" :key="type" @click="dataType=type">{{type}}</li>
            </ul>
          </div>
        </div>
    </div>
</template>
<script>
import CaretIcon from '@/components/UIElements/CaretIcon.vue';

export default {
  name: 'DataTypesHandler',
  props: ['enableDataType'],
  components: { CaretIcon },
  data() {
    return {
      showTypesList: false,
      dataType: 'string',
      dataTypes: ['string', 'date', 'boolean', 'long', 'double'],
    };
  },

  computed: {
  },

  created() {
    this.$on('clear-panel', this.clearPanel);
  },

  mounted() {
    this.$nextTick(() => {
    });
  },
  watch: {

  },
  methods: {
    getDataType() {
      return this.dataType;
    },
    clearPanel() {
      this.dataType = 'string';
    },
  },
};
</script>
<style scoped>

.inner-card{
  margin-top:8px;
}

.input-filter{
  margin-bottom: 4px;
  padding:0px;
  display: inline-flex;
  flex: 1;
  color:white;
}

li:hover{
  background-color: olivedrab;
}
  

.select-span{
  display: flex;
  justify-content: flex-end;  
}

.existing-types-list{
  height: 60px;
  overflow: scroll;
  margin-top:5px;
  margin-bottom:5px;
  padding:5px;
  color:white;
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

</style>
