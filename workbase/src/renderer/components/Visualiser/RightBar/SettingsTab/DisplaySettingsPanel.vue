<template>
    <div class="panel-container noselect">
        <div @click="toggleContent" class="panel-header">
            <vue-icon :icon="(showConceptDisplayContent && attributesLoaded) ?  'chevron-down' : 'chevron-right'" iconSize="14" className="vue-icon"></vue-icon>
            <h1>Display Settings</h1>
        </div>
        <div v-show="showConceptDisplayContent">
            <div class="panel-content" v-if="!currentKeyspace">
                Please select a keyspace
            </div>

            <div class="panel-content" v-else v-show="attributesLoaded">

                <div class="panel-content-item">
                    <div v-bind:class="(showTypeList) ? 'vue-button type-btn type-list-shown' : 'vue-button type-btn'" @click="toggleTypeList"><div class="type-btn-text" >{{currentType}}</div><vue-icon class="type-btn-caret" icon="caret-down"></vue-icon></div>
                </div>

                <div class="panel-list-item">
                    <div class="type-list" v-show="showTypeList">
                        <ul v-for="type in types" :key=type>
                            <li class="type-item" @click="selectType(type)" v-bind:class="[(type === currentType) ? 'type-item-selected' : '']">{{type}}</li>
                        </ul>
                    </div>
                </div>

                <div class="panel-content-item" v-bind:class="(showTypeList) ? 'disable-content' : ''">
                    <h1 class="sub-panel-header">
                        <div class="sub-title">Label</div>
                        <div class="vue-button reset-setting-btn" @click="toggleAttributeToLabel(undefined)"><vue-icon icon="eraser" iconSize="12"></vue-icon></div>
                    </h1>
                    <p v-if="!nodeAttributes.length">There are no attribute types available for this type of node.</p>
                    <ul v-else class="attribute-list">
                        <li :class="(currentTypeSavedAttributes.includes(prop)) ? 'attribute-btn toggle-attribute-btn' : 'attribute-btn'" @click="toggleAttributeToLabel(prop)" v-for="prop in nodeAttributes" :key=prop>
                            {{prop}}
                        </li>
                    </ul>
                </div>

                <div v-bind:class="(showTypeList) ? 'colour-item disable-content' : 'colour-item'">
                    <h1 class="sub-panel-header">
                        <div class="sub-title">Colour</div>
                        <div class="vue-button reset-setting-btn" @click="setTypeColour(undefined)"><vue-icon icon="eraser" iconSize="12"></vue-icon></div>
                    </h1>
                    <div class="row">
                        <chrome v-model="colour" :disableAlpha="true" :disableFields="true"></chrome>
                        <div>{{colour.hex}}</div>
                    </div>
                </div>
            </div>
        </div>
    </div>
</template>

<script>
  import { Chrome } from 'vue-color';
  import { mapGetters } from 'vuex';

  import { UPDATE_NODES_COLOUR, UPDATE_NODES_LABEL, OPEN_GRAKN_TX } from '@/components/shared/StoresActions';
  import DisplaySettings from './DisplaySettings';


  export default {
    name: 'DisplaySettingsPanel',
    components: { Chrome },
    data() {
      return {
        showConceptDisplayContent: true,
        showTypeList: false,
        types: [],
        currentType: null,
        nodeAttributes: [],
        currentTypeSavedAttributes: [],
        attributesLoaded: false,
        colour: {
          hex: '#563891',
        },
      };
    },
    created() {
      this.loadMetaTypes();
      this.loadAttributeTypes();
      this.loadColour();
    },
    computed: {
      ...mapGetters(['metaTypeInstances', 'selectedNode', 'currentKeyspace']),
      node() {
        if (this.selectedNode && this.selectedNode.baseType.includes('Type')) {
          return null;
        }
        return this.selectedNode;
      },
  
    },
    watch: {
      showConceptDisplayContent(open) {
        if (open && this.currentKeyspace) {
          this.loadMetaTypes();
          this.loadAttributeTypes();
          this.loadColour();
        }
      },
      currentType() {
        this.loadAttributeTypes();
        this.loadColour();
      },
      metaTypeInstances() {
        this.loadMetaTypes();
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
        if (!this.currentType) return;
        const graknTx = await this.$store.dispatch(OPEN_GRAKN_TX);

        const type = await graknTx.getSchemaConcept(this.currentType);

        this.nodeAttributes = await Promise.all((await (await type.attributes()).collect()).map(type => type.label()));
        this.nodeAttributes.sort();
        this.currentTypeSavedAttributes = DisplaySettings.getTypeLabels(this.currentType);

        graknTx.close();
        this.attributesLoaded = true;
      },
      loadMetaTypes() {
        if (this.metaTypeInstances.entities || this.metaTypeInstances.attributes) {
          this.types = this.metaTypeInstances.entities.concat(this.metaTypeInstances.attributes);
          this.currentType = this.types[0];
          this.showConceptDisplayContent = true;
        }
      },
      loadColour() {
        this.colour.hex = (DisplaySettings.getTypeColours(this.currentType).length) ? DisplaySettings.getTypeColours(this.currentType) : '#563891';
      },
      toggleAttributeToLabel(attribute) {
        // Persist changes into localstorage for current type
        DisplaySettings.toggleLabelByType({ type: this.currentType, attribute });
        this.$store.dispatch(UPDATE_NODES_LABEL, this.currentType);
        this.currentTypeSavedAttributes = DisplaySettings.getTypeLabels(this.currentType);
      },
      toggleContent() {
        this.showConceptDisplayContent = !this.showConceptDisplayContent;
      },
      toggleTypeList() {
        this.showTypeList = !this.showTypeList;
      },
      selectType(type) {
        this.showTypeList = false;
        this.currentType = type;
      },
      setTypeColour(col) {
        if (DisplaySettings.getTypeColours(this.currentType) !== col) {
          if (!col) this.colour.hex = 'default';
          DisplaySettings.toggleColourByType({ type: this.currentType, colourString: col });
          this.$store.dispatch(UPDATE_NODES_COLOUR, this.currentType);
        }
      },
    },
  };
</script>

<style scoped>

    .type-list-shown {
        border: 1px solid var(--button-hover-border-color) !important;
    }

    .panel-content {
        padding: var(--container-padding);
        display: flex;
        flex-direction: column;
        border-bottom: var(--container-darkest-border);
    }
    .panel-content-item {
        padding-bottom: var(--container-padding);
        margin: var(--container-padding) var(--container-padding) 0px;
        border-bottom: var(--container-light-border);
        display: flex;
        flex-direction: column;
        align-items: center;
    }

    .type-btn {
        height: 22px;
        min-height: 22px !important;
        cursor: pointer;
        display: flex;
        flex-direction: row;
        width: 100%;
        margin: 0px !important;
        z-index: 2;
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
        min-height: 22px;
        margin: 0px !important;
    }

    .type-list {
        border-left: var(--container-darkest-border);
        border-right: var(--container-darkest-border);
        border-bottom: var(--container-darkest-border);


        background-color: var(--gray-1);
        max-height: 137px;
        overflow: auto;
        position: absolute;
        width: 193px;
        margin-left: 5px;
        margin-top: -6px;
        z-index: 1;
    }


    .type-list::-webkit-scrollbar {
        width: 2px;
    }

    .type-list::-webkit-scrollbar-thumb {
        background: var(--green-4);
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


    .sub-panel-header {
        display: flex;
        align-items: center;
        width: 100%;
        height: var(--line-height);
        margin-top: -5px;
    }

    .sub-title {
        justify-content: center;
        display: flex;
        width: 100%;
    }

    .attribute-list {
        border: var(--container-darkest-border);
        background-color: var(--gray-1);
        width: 100%;
        max-height: 120px;
        overflow: auto;
    }

    .attribute-list::-webkit-scrollbar {
        width: 2px;
    }

    .attribute-list::-webkit-scrollbar-thumb {
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

    /*dynamic*/
    .colour-item {
        padding-bottom: var(--container-padding);
        margin: var(--container-padding) var(--container-padding) 0px;
        display: flex;
        flex-direction: column;
        align-items: center;
    }

    .row {
        display: flex;
        flex-direction: row;
        justify-content: space-between;
        align-items: center;
        width: 100%;
        height: var(--line-height);
    }

    /*dynamic*/
    .disable-content {
        pointer-events: none;
        opacity: 0.1;
    }
</style>
