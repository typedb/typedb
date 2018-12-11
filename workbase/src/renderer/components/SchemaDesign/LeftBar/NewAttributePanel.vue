<template>
  <div>
    <button class="btn define-btn" :class="(showPanel === 'attribute') ? 'green-border': ''" @click="togglePanel">Attribute Type</button>

    <div class="new-attribute-panel-container" v-if="showPanel === 'attribute'">
      <div class="title">
        Define New Attribute Type
        <div class="close-cross" @click="$emit('show-panel', undefined)"><vue-icon icon="cross" iconSize="12" className="tab-icon"></vue-icon></div>
      </div>

      <div class="content">
        <div class="row">
          <input class="input-small label-input" v-model="attributeLabel" placeholder="Attribute Label">
          <div class="sub-label">sub</div>
          <div class="btn type-btn" :class="(showAttributeTypeList) ? 'type-list-shown' : ''" @click="showAttributeTypeList = !showAttributeTypeList"><div class="type-btn-text" >{{superType}}</div><div class="type-btn-caret"><vue-icon className="vue-icon" icon="caret-down"></vue-icon></div></div>
          
          <div class="type-list" v-show="showAttributeTypeList">
            <ul v-for="type in superTypes" :key=type>
                <li class="type-item" @click="selectSuperType(type)" :class="[(type === superType) ? 'type-item-selected' : '']">{{type}}</li>
            </ul>
          </div>
        </div>

        <div class="row">
          <div class="data-type-options">
            <div class="list-label">data type</div>
            <div v-if="superType === 'attribute'" class="btn data-type-btn" :class="(showDataTypeList) ? 'type-list-shown' : ''" @click="toggleDataList"><div class="type-btn-text" >{{dataType}}</div><div class="type-btn-caret"><vue-icon className="vue-icon" icon="caret-down"></vue-icon></div></div>
            <div v-else class="inherited-data-type">{{dataType}}</div>

            <div class="data-type-list" v-show="showDataTypeList">
                <ul v-for="type in dataTypes" :key=type>
                    <li class="type-item" @click="selectDataType(type)" :class="[(type === dataType) ? 'type-item-selected' : '']">{{type}}</li>
                </ul>
            </div>
          </div> 
        </div>

        <div class="row">
          <div @click="showHasPanel = !showHasPanel" class="has-header">
            <vue-icon :icon="(showHasPanel) ?  'chevron-down' : 'chevron-right'" iconSize="14" className="vue-icon"></vue-icon>
            has
            </div>
        </div>

        <div class="row-2" v-if="showHasPanel">
          <div class="has">
            <ul class="attribute-type-list" v-if="metaTypeInstances.attributes.length">
              <li :class="(toggledAttributeTypes.includes(attributeType)) ? 'attribute-btn toggle-attribute-btn' : 'attribute-btn'" @click="toggleAttributeType(attributeType)" v-for="attributeType in metaTypeInstances.attributes" :key=attributeType>
                  {{attributeType}}
              </li>
            </ul>
            <div v-else class="no-types">There are no attribute types defined</div>
          </div>
        </div>
    
        <div class="row">
          <div @click="showPlaysPanel = !showPlaysPanel" class="has-header">
            <vue-icon :icon="(showPlaysPanel) ?  'chevron-down' : 'chevron-right'" iconSize="14" className="vue-icon"></vue-icon>
            plays
            </div>
        </div>

        <div class="row-2" v-if="showPlaysPanel">
          <div class="has">
            <ul class="attribute-type-list" v-if="metaTypeInstances.roles.length">
              <li :class="(toggledRoleTypes.includes(roleType)) ? 'attribute-btn toggle-attribute-btn' : 'attribute-btn'" @click="toggleRoleType(roleType)" v-for="roleType in metaTypeInstances.roles" :key=roleType>
                  {{roleType}}
              </li>
            </ul>
            <div v-else class="no-types">There are no role types defined</div>
          </div>
        </div>

        <div class="submit-row">
          <button class="btn submit-btn" @click="resetPanel">Clear</button>
          <loading-button v-on:clicked="defineAttributeType" text="Submit" :loading="showSpinner" className="btn submit-btn"></loading-button>
        </div>

      </div>
    </div>
  </div>
</template>

<style scoped>

  .no-types {
    background-color: var(--gray-1);
    padding: var(--container-padding);
    border: var(--container-darkest-border);
    border-top: 0px;
  }

  .inherited-data-type {
    height: 22px;
    display: flex;
    align-items: center;
    border: var(--container-darkest-border);
    width: 100%;
    padding-left: 4px;
  }

  .submit-row {
    justify-content: space-between;
    display: flex;
    flex-direction: row;
    align-items: center;
    padding: var(--container-padding);
  }

  .has-header {
    width: 100%;
    background-color: var(--gray-1);
    border: var(--container-darkest-border);
    height: 22px;
    display: flex;
    align-items: center;
    cursor: pointer;
  }

  .has {
    width: 100%;
  }

  .attribute-type-list {
    border: var(--container-darkest-border);
    background-color: var(--gray-1);
    width: 100%;
    max-height: 140px;
    overflow: auto;
  }

  .attribute-type-list::-webkit-scrollbar {
    width: 2px;
  }

  .attribute-type-list::-webkit-scrollbar-thumb {
    background: var(--green-4);
  }

  /*dynamic*/
  .attribute-btn {
    align-items: center;
    padding: 2px;
    cursor: pointer;
    white-space: normal;
    word-wrap: break-word;
  }

  /*dynamic*/
  .attribute-btn:hover {
    background-color: var(--purple-4);
  }

  /*dynamic*/
  .toggle-attribute-btn {
    background-color: var(--purple-3);
  }

  .row-2 {
    display: flex;
    flex-direction: row;
    padding: 0px var(--container-padding) 0px var(--container-padding);
    justify-content: space-between;
  }

  .plays {
    width: 140px;
  }

  .submit-div {
    padding-bottom: var(--container-padding);
  }

  .green-border {
    border: 1px solid var(--button-hover-border-color);
  }

  .close-cross {
    position: absolute;
    right: 2px;
  }

  .sub-label {
    margin-left: 5px;
    margin-right: 5px;
  }

  .data-type-btn {
    width: 100% !important;
    height: 22px;
    min-height: 22px !important;
    cursor: pointer;
    display: flex;
    flex-direction: row;
    width: 140px;
    z-index: 2;
    margin: 0px 0px 0px 0px !important;
  }

  .data-type-options {
    margin-left: 145px;
    display: flex;
    align-items: center;
    width: 100%;
  }
  
  .list-label {
    margin-right: 5px;
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
    border-bottom: var(--container-darkest-border);
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
    z-index: 3;
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
    right: 10px;
    top: 82px;
    width: 111.5px;
    z-index: 1;
  }
  
  .type-list-shown {
    border: 1px solid var(--button-hover-border-color) !important;
  }

  .override-datatype{
    background-color: var(--gray-2) !important;
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
  import { DEFINE_ATTRIBUTE_TYPE, OPEN_GRAKN_TX } from '@/components/shared/StoresActions';
  import { createNamespacedHelpers } from 'vuex';

  export default {
    props: ['showPanel'],
    data() {
      return {
        attributeLabel: '',
        showAttributeTypeList: false,
        superTypes: [],
        superType: undefined,
        showDataTypeList: false,
        dataTypes: ['string', 'long', 'double', 'boolean', 'date'],
        dataType: undefined,
        showSpinner: false,
        toggledAttributeTypes: [],
        toggledRoleTypes: [],
        showHasPanel: false,
        showPlaysPanel: false,
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
        ...mapActions([DEFINE_ATTRIBUTE_TYPE, OPEN_GRAKN_TX]),
      };
    },
    watch: {
      showPanel(val) {
        if (val === 'attribute') { // reset panel when it is toggled
          this.resetPanel();
        }
      },
      async superType(val) {
        if (val !== 'attribute') { // if super type is not 'attribute' set data type of super type
          const graknTx = await this[OPEN_GRAKN_TX]();
          const attributeType = await graknTx.getSchemaConcept(val);
          this.dataType = (await attributeType.dataType()).toLowerCase();
          this.showDataTypeList = false;
        } else {
          this.dataType = this.dataTypes[0];
        }
      },
    },
    methods: {
      defineAttributeType() {
        if (this.attributeLabel === '') {
          this.$notifyError('Cannot define Attribute Type without Attribute Label');
        } else {
          this.showSpinner = true;
          this[DEFINE_ATTRIBUTE_TYPE]({
            attributeLabel: this.attributeLabel, superType: this.superType, dataType: this.dataType, attributeTypes: this.toggledAttributeTypes, roleTypes: this.toggledRoleTypes,
          })
            .then(() => {
              this.showSpinner = false;
              this.superTypes.push(this.attributeLabel);
              this.$notifyInfo(`Attribute Type, ${this.attributeLabel}, has been defined`);
              this.resetPanel();
            })
            .catch((e) => {
              logger.error(e.stack);
              this.showSpinner = false;
              if (e.stack.includes('ALREADY_EXISTS')) this.$notifyError(`Attribute Type with label, ${this.attributeLabel}, already exists. Please choose a different label`);
            });
        }
      },
      selectSuperType(type) {
        this.superType = type;
        this.showAttributeTypeList = false;
      },
      selectDataType(type) {
        this.dataType = type;
        this.showDataTypeList = false;
      },
      resetPanel() {
        this.attributeLabel = '';
        this.showAttributeTypeList = false;
        this.showDataTypeList = false;
        this.superTypes = ['attribute', ...this.metaTypeInstances.attributes];
        this.superType = this.superTypes[0];
        this.dataType = this.dataTypes[0];
        this.toggledAttributeTypes = [];
        this.toggledRoleTypes = [];
        this.showHasPanel = false;
        this.showPlaysPanel = false;
      },
      togglePanel() {
        if (this.showPanel === 'attribute') this.$emit('show-panel', undefined);
        else this.$emit('show-panel', 'attribute');
      },
      toggleDataList() {
        if (this.superType === 'attribute') this.showDataTypeList = !this.showDataTypeList;
      },
      toggleAttributeType(type) {
        const index = this.toggledAttributeTypes.indexOf(type);
        if (index > -1) {
          this.toggledAttributeTypes.splice(index, 1);
        } else {
          this.toggledAttributeTypes.push(type);
        }
      },
      toggleRoleType(type) {
        const index = this.toggledRoleTypes.indexOf(type);
        if (index > -1) {
          this.toggledRoleTypes.splice(index, 1);
        } else {
          this.toggledRoleTypes.push(type);
        }
      },
    },
  };
</script>
