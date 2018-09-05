<template>
    <div class="panel-container">
        <div @click="toggleContent" class="panel-header" v-bind:style="{ opacity: (this.currentKeyspace) ? 1 : 0.5 }">
            <vue-icon :icon="(showConceptDisplayContent && attributesLoaded) ?  'chevron-down' : 'chevron-right'" iconSize="14"></vue-icon>
            <h1>CONCEPT DISPLAY</h1>
        </div>
        <div class="content" v-show="showConceptDisplayContent && attributesLoaded">
            <div class="content-item">
                <h1 class="label">TYPE</h1>
                <vue-popover :button="typesBtn" :items="types" v-on:emit-item="selectType"></vue-popover>
            </div>
            <div class="attributes-item">
                <h1 class="label">ATTRIBUTES</h1>
                <div class="column">
                    <p v-if="!nodeAttributes.length">There are no attribute types available for this type of node.</p>
                    <ul class="attribute-list">
                        <li class="attribute" @click="toggleAttributeToLabel(prop)" v-for="prop in nodeAttributes" :key=prop>
                            <vue-button :text="prop" :className="(currentTypeSavedAttributes.includes(prop)) ? 'toggle-attribute-btn' : 'vue-button'"></vue-button>
                        </li>
                    </ul>
                    <vue-button v-if="nodeAttributes.length" icon="refresh" className="vue-button" v-on:clicked="toggleAttributeToLabel"></vue-button>
                </div>
            </div>
            <div class="color-picker">
                <chrome v-model="colour" :disableAlpha="true" :disableFields="true"></chrome>
                <!--<slider v-model="colour"></slider>-->
                <div class="row">
                    <div>COLOR: {{colour.hex}}</div>
                    <vue-button class="reset-color-btn" icon="refresh" v-on:clicked="setTypeColour" className="vue-button"></vue-button>
                </div>
            </div>
        </div>
    </div>
</template>

<script>
  import { Chrome, Slider } from 'vue-color';
  import * as React from 'react';
  import { Button } from '@blueprintjs/core';

  import { TOGGLE_COLOUR, TOGGLE_LABEL } from '@/components/shared/StoresActions';
  import NodeSettings from './NodeSettings';


  export default {
    name: 'ConceptDisplayPanel',
    components: { Chrome, Slider },
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
          this.attributesLoaded = false;
          await this.loadAttributeTypes();
          this.loadColour();
          this.attributesLoaded = true;
        }
      },
      metaTypeInstances(metaTypes) {
        if (metaTypes.entities.length || metaTypes.attributes.length || metaTypes.relationships.length) {
          this.types.push(...metaTypes.entities, ...metaTypes.attributes, ...metaTypes.relationships);
          this.currentType = this.types[0];
          this.showConceptDisplayContent = true;
        } else { this.showConceptDisplayContent = false; }
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
      selectType(type) {
        this.currentType = type;
      },
      renderButton() {
        this.typesBtn = React.createElement(Button, {
          text: this.currentType,
          className: 'vue-button',
          key: this.currentType,
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

    .content {
        padding: var(--container-padding);
        display: flex;
        flex-direction: column;
        justify-content: center;
    }

    .content-item {
        padding: var(--container-padding);
        display: flex;
        flex-direction: row;
        align-items: center;
    }

    .attributes-item {
        padding: var(--container-padding);
        display: flex;
        flex-direction: row;
        margin-bottom: 10px;
    }

    .attribute-list {
        max-height: 150px;
        overflow: scroll;
        margin-bottom: 10px;
    }

    .label {
        margin-right: 20px;
        width: 65px;
    }

    .color-picker {
        display: flex;
        align-items: center;
        flex-direction: column;
        border-top: var(--container-dark-border);
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
