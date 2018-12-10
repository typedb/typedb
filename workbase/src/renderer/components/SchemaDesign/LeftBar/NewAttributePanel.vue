<template>
  <div>
    <button class="btn define-btn" @click="togglePanel">Attribute Type</button>
    <div class="new-attribute-panel-container" v-if="panelShown === 'attribute'">
      <div class="title">
        Define New Attribute Type
        <div class="close-container" @click="$emit('show-panel', undefined)"><vue-icon icon="cross" iconSize="12" className="tab-icon"></vue-icon></div>
      </div>
      <div class="content">

        <div class="row">

          <input class="input-small label-input" v-model="label" placeholder="Attribute Label">
          <div class="sub-label">sub</div>
          <div v-bind:class="(showTypeList) ? 'btn type-btn type-list-shown' : 'btn type-btn'" @click="showTypeList = !showTypeList"><div ref="typeText" class="type-btn-text" >{{superType}}</div><div class="type-btn-caret"><vue-icon className="vue-icon" icon="caret-down"></vue-icon></div></div>
          
          <div class="type-list" v-show="showTypeList">
            <ul v-for="type in types" :key=type>
                <li class="type-item" @click="selectSuperType(type)" v-bind:class="[(type === superType) ? 'type-item-selected' : '']">{{type}}</li>
            </ul>
          </div>

        </div>

        <div class="row">

          <div class="data-type-options" v-if="showDataTypeOptions">
            <div class="list-label">data type</div>
            <div v-bind:class="(showDataTypeList) ? 'btn type-btn type-list-shown data-type-btn' : 'btn type-btn data-type-btn'" @click="showDataTypeList = !showDataTypeList"><div class="type-btn-text" >{{selectedDataType}}</div><div class="type-btn-caret"><vue-icon className="vue-icon" icon="caret-down"></vue-icon></div></div>

            <div class="data-type-list" v-show="showDataTypeList">
                <ul v-for="dataType in dataTypes" :key=dataType>
                    <li class="type-item" @click="selectDataType(dataType)" v-bind:class="[(dataType === selectedDataType) ? 'type-item-selected' : '']">{{dataType}}</li>
                </ul>
            </div>
            
          </div>
            <div class="submit-btn-div">
              <loading-button v-on:clicked="defineAttributeType" text="Submit" :loading="showSpinner" className="btn submit-btn"></loading-button>
            </div>
        </div>
        
        <div class="row submit-row">
        </div>

      </div>
    </div>
  </div>
</template>

<style scoped>

    .close-container {
        position: absolute;
        right: 2px;
    }


      .submit-row {
  justify-content: flex-end;
    display: flex;
    flex-direction: row;
    align-items: center;
    padding: var(--container-padding);
    }


.sub-label {
  margin-left: 5px;
  margin-right: 5px;
}

.data-type-btn {
  width: 88px !important;
}

.data-type-options {
  display: flex;
  align-items: center;

}
  .list-label {
    margin-right: 5px;
  }

    .submit-row {
      justify-content: space-between !important;
    }

  .label-input {
    width: 140px;
  }

  .row {
    display: flex;
    flex-direction: row;
    align-items: center;
    padding: var(--container-padding) var(--container-padding) 0px var(--container-padding);
    justify-content: space-between;
    white-space: nowrap;
  }

  .new-attribute-panel-container {
    position: absolute;
    left: 120px;
    top: 10px;
    background-color: var(--gray-2);
    border: var(--container-darkest-border);
  }

  .title {
    background-color: var(--gray-1); 
    display: flex;
    align-items: center;
    padding: var(--container-padding);
  }

  .content {
    padding: var(--container-padding);
  }

  .type-list {
      border-left: var(--container-darkest-border);
      border-right: var(--container-darkest-border);
      border-bottom: var(--container-darkest-border);


      background-color: var(--gray-1);
      max-height: 172px;
      overflow: auto;
      position: absolute;
      right: 10px;
      top: 54px;
      width: 140px;
      z-index: 1;
  }


  .type-list::-webkit-scrollbar {
      width: 2px;
  }

  .type-list::-webkit-scrollbar-thumb {
      background: var(--green-4);
  }

    .data-type-list {
      border-left: var(--container-darkest-border);
      border-right: var(--container-darkest-border);
      border-bottom: var(--container-darkest-border);


      background-color: var(--gray-1);
      max-height: 174px;
      position: absolute;
      left: 62px;
      top: 90px;
      width: 87px;
      z-index: 1;
  }
  
  .type-list-shown {
    border: 1px solid var(--button-hover-border-color) !important;
  }

  .type-item {
      align-items: center;
      padding: 2px;
      cursor: pointer;
      white-space: normal;
      word-wrap: break-word;
  }

  .type-item:hover {
      background-color: var(--purple-4);
  }

  /*dynamic*/
  .type-item-selected {
      background-color: var(--purple-3);
  }

  .type-btn {
      height: 22px;
      min-height: 22px !important;
      cursor: pointer;
      display: flex;
      flex-direction: row;
      width: 140px;
      z-index: 2;
      margin: 0px 0px 0px 0px !important;
  }

  .type-btn-text {
      width: 100%;
      padding-left: 4px;
      display: block;
      white-space: normal !important;
      word-wrap: break-word;
      line-height: 19px;
      overflow: -webkit-paged-x;
  }


    .type-btn-text::-webkit-scrollbar {
      height: 2px;
  }

  .type-btn-text::-webkit-scrollbar-thumb {
      background: var(--green-4);
  }

  .type-btn-caret {
      cursor: pointer;
      align-items: center;
      display: flex;
  }

</style>

<script>
  import logger from '@/../Logger';
  import { DEFINE_ATTRIBUTE_TYPE, OPEN_GRAKN_TX, UPDATE_METATYPE_INSTANCES } from '@/components/shared/StoresActions';
  import { createNamespacedHelpers } from 'vuex';

  export default {
    props: ['panelShown'],
    data() {
      return {
        showTypeList: false,
        showDataTypeList: false,
        types: ['attribute'],
        dataTypes: ['string', 'long', 'double', 'boolean', 'date'],
        inheritDatatype: undefined,
        selectedDataType: undefined,
        superType: undefined,
        label: '',
        showSpinner: false,
        showDataTypeOptions: true,
      };
    },
    beforeCreate() {
      const { mapGetters, mapActions } = createNamespacedHelpers('schema-design');

      // computed
      this.$options.computed = {
        ...(this.$options.computed || {}),
        ...mapGetters(['metaTypeInstances']),
      };

      // methods
      this.$options.methods = {
        ...(this.$options.methods || {}),
        ...mapActions([DEFINE_ATTRIBUTE_TYPE, OPEN_GRAKN_TX, UPDATE_METATYPE_INSTANCES]),
      };
    },
    watch: {
      panelShown(val) {
        if (val && this.superType === undefined) {
          this.superType = this.types[0];
          this.selectedDataType = this.dataTypes[0];
          this.types.push(...this.metaTypeInstances.attributes);
        }
      },
      async superType(val) {
        if (val !== 'attribute') {
          const graknTx = await this[OPEN_GRAKN_TX]();
          const attributeType = await graknTx.getSchemaConcept(val);
          this.selectedDataType = (await attributeType.dataType()).toLowerCase();
          this.inheritDatatype = this.selectedDataType;
          this.dataTypes = [];
        } else {
          this.dataTypes = ['string', 'long', 'double', 'boolean', 'date'];
        }
      },
    },
    methods: {
      defineAttributeType() {
        this.showSpinner = true;
        this[DEFINE_ATTRIBUTE_TYPE]({ label: this.label, superType: this.superType, dataType: this.selectedDataType, inheritDatatype: this.inheritDatatype })
          .then(() => {
            this.showSpinner = false;
            this.types.push(this.label);
            this.clearPanel();
          })
          .catch((e) => {
            logger.error(e.stack);
            this.showSpinner = false;
          });
      },
      selectSuperType(type) {
        this.superType = type;
        this.showTypeList = false;
      },
      selectDataType(type) {
        this.selectedDataType = type;
        this.showDataTypeList = false;
      },
      clearPanel() {
        this.label = '';
        this.superType = this.types[0];
        this.selectedDataType = this.dataTypes[0];
      },
      togglePanel() {
        if (this.panelShown === 'attribute') this.$emit('show-panel', undefined);
        else this.$emit('show-panel', 'attribute');
      },
    },
  };
</script>
