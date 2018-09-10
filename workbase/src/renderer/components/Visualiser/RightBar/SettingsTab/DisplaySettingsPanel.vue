<template>
    <div class="panel-container">
        <div @click="toggleContent" class="panel-header">
            <vue-icon :icon="(showConceptDisplayContent && attributesLoaded) ?  'chevron-down' : 'chevron-right'" iconSize="14"></vue-icon>
            <h1>Display Settings</h1>
        </div>
        <div class="content" v-show="showConceptDisplayContent && attributesLoaded">



            <div class="content-item">
                <h1 class="label">TYPE</h1>
                <!--<vue-popover :button="typesBtn" :items="types" v-on:emit-item="selectType"></vue-popover>-->
                <div class="vue-button type-btn" @click="toggleTypeList"><div class="type-btn-text" >{{currentType}}</div><vue-icon class="type-btn-caret" icon="caret-down"></vue-icon></div>

            </div>
            <div class="type-list" v-show="showTypeList">
                <ul v-for="type in types" :key=type>
                    <li class="type-item" @click="selectType(type)" v-bind:class="[(type === currentType) ? 'type-item-selected' : '']">{{type}}</li>
                </ul>
            </div>



            <div class="attributes-item">
                <h1 class="label">ATTRIBUTES</h1>
                <div class="column">
                    <p v-if="!nodeAttributes.length">There are no attribute types available for this type of node.</p>
                    <ul class="attribute-list">
                        <li :class="(currentTypeSavedAttributes.includes(prop)) ? 'toggle-attribute-btn' : 'attribute-btn'" @click="toggleAttributeToLabel(prop)" v-for="prop in nodeAttributes" :key=prop>
                            {{prop}}
                            <!--<vue-button :text="prop" :className="(currentTypeSavedAttributes.includes(prop)) ? 'vue-button toggle-attribute-btn' : 'vue-button attribute-btn'"></vue-button>-->
                        </li>
                    </ul>
                    <vue-button v-if="nodeAttributes.length" icon="refresh" className="vue-button" v-on:clicked="toggleAttributeToLabel"></vue-button>
                </div>
            </div>
            <div class="color-picker">
                <chrome v-model="colour" :disableAlpha="true" :disableFields="true"></chrome>
                <div class="row">
                    <div>COLOR: {{colour.hex}}</div>
                    <vue-button class="reset-color-btn" icon="refresh" v-on:clicked="setTypeColour" className="vue-button"></vue-button>
                </div>
            </div>
        </div>
    </div>
</template>

<script>
  import { Chrome } from 'vue-color';
  import * as React from 'react';
  import { Button } from '@blueprintjs/core';

  import { TOGGLE_COLOUR, TOGGLE_LABEL } from '@/components/shared/StoresActions';
  import NodeSettings from './DisplaySettings';


  export default {
    name: 'ConceptDisplayPanel',
    components: { Chrome },
    props: ['localStore'],
    data() {
      return {
        showConceptDisplayContent: false,
        showTypeList: false,
        types: [],
        currentType: null,
        typesBtn: null,
        nodeAttributes: [],
        currentTypeSavedAttributes: [],
        attributesLoaded: false,
        colour: {
          hex: '#563891',
        },
      };
    },
    created() {
      this.renderButton();
    },
    computed: {
      metaTypeInstances() {
        return this.localStore.getMetaTypeInstances();
      },
      node() {
        let node = this.localStore.getSelectedNode();

        if (node && node.baseType.includes('Type')) {
          node = null;
        }
        return node;
      },
      currentKeyspace() {
        return this.localStore.getCurrentKeyspace();
      },
    },
    watch: {
      async showConceptDisplayContent(open) {
        if (open) {
          this.loadMetaTypes();
          this.attributesLoaded = false;
          await this.loadAttributeTypes();
          this.loadColour();
          this.attributesLoaded = true;
        }
      },
      currentType() {
        this.renderButton();
        this.loadAttributeTypes();
        this.loadColour();
      },
      node(node) {
        if (node) this.currentType = node.type;
      },
      colour(col) {
        this.setTypeColour(col.hex);
      },
    },
    methods: {
      async loadAttributeTypes() {
        const type = await this.localStore.graknTx.getSchemaConcept(this.currentType);

        this.nodeAttributes = await Promise.all((await (await type.attributes()).collect()).map(type => type.label()));
        this.nodeAttributes.sort();
        this.currentTypeSavedAttributes = NodeSettings.getTypeLabels(this.currentType);
      },
      loadMetaTypes() {
        if (this.metaTypeInstances.entities.length || this.metaTypeInstances.attributes.length || this.metaTypeInstances.relationships.length) {
          this.types.push(...this.metaTypeInstances.entities, ...this.metaTypeInstances.attributes, ...this.metaTypeInstances.relationships);
          this.currentType = this.types[0];
          this.showConceptDisplayContent = true;
        } else { this.showConceptDisplayContent = false; }
      },
      loadColour() {
        this.colour.hex = (NodeSettings.getTypeColours(this.currentType).length) ? NodeSettings.getTypeColours(this.currentType) : 'default';
      },
      toggleAttributeToLabel(attribute) {
        // Persist changes into localstorage for current type
        NodeSettings.toggleLabelByType({ type: this.currentType, attribute });
        this.localStore.dispatch(TOGGLE_LABEL, this.currentType);
        this.currentTypeSavedAttributes = NodeSettings.getTypeLabels(this.currentType);
      },
      toggleContent() {
        if (this.currentKeyspace) this.showConceptDisplayContent = !this.showConceptDisplayContent;
      },
      toggleTypeList() {
        this.showTypeList = !this.showTypeList;
      },
      selectType(type) {
        this.showTypeList = false;
        this.currentType = type;
      },
      renderButton() {
        this.typesBtn = React.createElement(Button, {
          text: this.currentType,
          className: 'vue-button attribute-btn',
          key: this.currentType,
          rightIcon: 'caret-down',
        });
      },
      setTypeColour(col) {
        if (NodeSettings.getTypeColours(this.currentType) !== col) {
          if (!col) this.colour.hex = 'default';
          NodeSettings.toggleColourByType({ type: this.currentType, colourString: col });
          this.localStore.dispatch(TOGGLE_COLOUR, this.currentType);
        }
      },
    },
  };
</script>

<style scoped>

    .attribute-btn {

    }

    .attribute-btn:hover {
        background-color: var(--gray-3);
    }

    .toggle-attribute-btn {
        background-color: var(--gray-2);
    }

    .type-btn {
        min-height: 22px;
        cursor: pointer;
        display: flex;
        flex-direction: row;
    }

    .type-btn-text {
        width: 80px;
        padding-left: 3px;
        display: block;
        white-space: normal !important;
        word-wrap: break-word;
        line-height: 22px;
    }

    .type-btn-caret {
        cursor: pointer;
        align-items: center;
        display: flex;
        min-height: 22px;
        margin-left: 0px !important;
    }




    .type-item {
        align-items: center;
        padding: 2px;
        cursor: pointer;
        white-space: normal;
        word-wrap: break-word;
    }

    .type-item:hover {
        background-color: var(--gray-2);
    }

    .type-item-selected {
        background-color: var(--gray-3);
    }





    .type-list {
        border: var(--container-darkest-border);
        background-color: var(--gray-1);
        margin-left: 88px;
        margin-top: -8px;
        width: 98px;
        max-height: 100px;
        overflow: auto;
    }





    .content {
        padding: var(--container-padding);
        display: flex;
        flex-direction: column;
        justify-content: center;
        border-bottom: var(--container-darkest-border);

    }

    .content-item {
        padding: var(--container-padding);
        display: flex;
        flex-direction: row;
        align-items: center;
    }

    .attributes-item {
        align-items: center;
        padding: 2px;
        cursor: pointer;
        white-space: normal;
        word-wrap: break-word;
    }

    .attribute-list {
        border: var(--container-darkest-border);
        background-color: var(--gray-1);
        margin-left: 88px;
        margin-top: -8px;
        width: 98px;
        max-height: 100px;
        overflow: auto;
    }

    .label {
        margin-right: 20px;
        width: 60px;
    }

    .color-picker {
        display: flex;
        align-items: center;
        flex-direction: column;
        border-top: var(--container-light-border);
        justify-content: center;
    }

    .row {
        display: flex;
        flex-direction: row;
        align-items: center;
    }
    .column {
        display: flex;
        flex-direction: column;
    }

    .reset-color-btn {
        margin-left: 10px;
    }
</style>
