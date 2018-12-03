<template>
  <div>
    <button class="btn define-btn" @click="$emit('show-panel', 'entity')">E</button>
    <div class="new-entity-panel-container" v-if="panelShown === 'entity'">
      <div class="title">Define New Entity Type</div>
      <div class="content">

        <div class="row">
          <input class="input-small label-input" v-model="label" placeholder="Label">
          sub
          <div v-bind:class="(showTypeList) ? 'btn type-btn type-list-shown' : 'btn type-btn'" @click="showTypeList = !showTypeList"><div class="type-btn-text" >{{superType}}</div><div class="type-btn-caret"><vue-icon className="vue-icon" icon="caret-down"></vue-icon></div></div>

          <div class="type-list" v-show="showTypeList">
              <ul v-for="type in types" :key=type>
                  <li class="type-item" @click="selectSuperType(type)" v-bind:class="[(type === superType) ? 'type-item-selected' : '']">{{type}}</li>
              </ul>
          </div>
        </div>

        <div class="row">
          <div class="has">has</div>
          <div class="plays">plays</div>
        </div>

        <div class="row">
          <div class="has">
            <ul class="attribute-type-list" v-if="metaTypeInstances.attributes.length">
              <li :class="(toggledAttributeTypes.includes(attributeType)) ? 'attribute-btn toggle-attribute-btn' : 'attribute-btn'" @click="toggleAttributeType(attributeType)" v-for="attributeType in metaTypeInstances.attributes" :key=attributeType>
                  {{attributeType}}
              </li>
            </ul>
            <div v-else>There are no attribute types defined</div>
          </div>
          <div class="plays">
            <ul class="attribute-type-list" v-if="metaTypeInstances.roles.length">
              <li :class="(toggledRoleTypes.includes(roleType)) ? 'attribute-btn toggle-attribute-btn' : 'attribute-btn'" @click="toggleRoleType(roleType)" v-for="roleType in metaTypeInstances.roles" :key=roleType>
                  {{roleType}}
              </li>
            </ul>
            <div v-else>There are no role types defined</div>
          </div>
        </div>

        <div class="row submit-row">
          <button class="btn" @click="clearPanel">Clear</button>
          <loading-button v-on:clicked="defineEntityType" text="Submit" :loading="showSpinner" className="btn submit-btn"></loading-button>
        </div>

      </div>
    </div>
  </div>
</template>

<style scoped>

    .submit-row {
      justify-content: space-between !important;
    }

    .attribute-type-list {
        border: var(--container-darkest-border);
        background-color: var(--gray-1);
        width: 140px;
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
        /*border: 1px solid var(--button-hover-border-color);*/
        background-color: var(--purple-3);
    }

  .plays {
    width: 140px;
  }

  .label-input {
    width: 140px;
    margin-right: 5px;
  }

  .row {
    display: flex;
    flex-direction: row;
    align-items: center;
    padding: var(--container-padding);
    justify-content: space-between;
  }

  .new-entity-panel-container {
    position: absolute;
    left: 42px;
    top: 10px;
    background-color: var(--gray-3);
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
      margin: 0px 0px 0px 5px !important;
  }

  .type-btn-text {
      width: 100%;
      padding-left: 4px;
      display: block;
      white-space: normal !important;
      word-wrap: break-word;
      line-height: 19px;
  }

  .type-btn-caret {
      cursor: pointer;
      align-items: center;
      display: flex;
  }

</style>

<script>
  import { DEFINE_ENTITY_TYPE } from '@/components/shared/StoresActions';
  import { createNamespacedHelpers } from 'vuex';

  export default {
    props: ['panelShown'],
    data() {
      return {
        showTypeList: false,
        types: ['entity'],
        toggledAttributeTypes: [],
        toggledRoleTypes: [],
        superType: undefined,
        label: '',
        showSpinner: false,
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
        ...mapActions([DEFINE_ENTITY_TYPE]),
      };
    },
    created() {
      this.superType = this.types[0];
      this.types.push(...this.metaTypeInstances.entities);
    },
    methods: {
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
      async defineEntityType() {
        this.showSpinner = true;
        await this[DEFINE_ENTITY_TYPE]({ label: this.label, superType: this.superType, attributeTypes: this.toggledAttributeTypes, roleTypes: this.toggledRoleTypes });
        this.showSpinner = false;
      },
      selectSuperType(type) {
        this.superType = type;
        this.showTypeList = false;
      },
      clearPanel() {
        this.label = '';
        this.superType = this.types[0];
        this.toggledAttributeTypes = [];
        this.toggledRoleTypes = [];
      },
    },
  };
</script>
