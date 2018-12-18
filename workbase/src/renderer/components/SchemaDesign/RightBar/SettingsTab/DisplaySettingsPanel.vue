<template>
    <div class="panel-container noselect">
        <div @click="showConceptDisplayContent = !showConceptDisplayContent" class="panel-header">
            <vue-icon :icon="(showConceptDisplayContent) ?  'chevron-down' : 'chevron-right'" iconSize="14" className="vue-icon"></vue-icon>
            <h1>Display Settings</h1>
        </div>
        <div v-show="showConceptDisplayContent">
            <div class="panel-content" v-if="!currentKeyspace">
                Please select a keyspace
            </div>

            <div class="panel-content" v-else>
                <div class="panel-content-item">
                    <h1 class="panel-label">Entity types:</h1>
                    <div class="panel-value load-roleplayers-switch"><vue-switch :isToggled="showEntities" v-on:toggled="showEntities = !showEntities"></vue-switch></div>
                </div>
                <div class="panel-content-item">
                    <h1 class="panel-label">Attribute Types:</h1>
                    <div class="panel-value load-roleplayers-switch"><vue-switch :isToggled="showAttributes" v-on:toggled="showAttributes = !showAttributes"></vue-switch></div>
                </div>
                <div class="panel-content-item">
                    <h1 class="panel-label">Relationship Types:</h1>
                    <div class="panel-value load-roleplayers-switch"><vue-switch :isToggled="showRelationships" v-on:toggled="showRelationships = !showRelationships"></vue-switch></div>
                </div>
        
                
            </div>
        </div>
    </div>
</template>

<script>
  import { createNamespacedHelpers } from 'vuex';

  export default {
    name: 'DisplaySettingsPanel',
    components: { },
    data() {
      return {
        showConceptDisplayContent: true,
        showEntities: true,
        showAttributes: true,
        showRelationships: true,
      };
    },
    beforeCreate() {
      const { mapGetters } = createNamespacedHelpers('schema-design');

      // computed
      this.$options.computed = {
        ...(this.$options.computed || {}),
        ...mapGetters(['currentKeyspace', 'visFacade']),
      };
    },
    watch: {
      showEntities(val) {
        this.showTypes(val, 'ENTITY_TYPE');
      },
      showAttributes(val) {
        this.showTypes(val, 'ATTRIBUTE_TYPE');
      },
      showRelationships(val) {
        this.showTypes(val, 'RELATIONSHIP_TYPE');
      },
    },
    methods: {
      showTypes(showType, baseType) {
        const schemaConcepts = this.visFacade.getAllNodes().filter((x => x.baseType === baseType)).map(x => Object.assign(x, { hidden: !showType }));
        this.visFacade.updateNode(schemaConcepts);
      },
    },
};
</script>

<style scoped>

    .panel-content {
        padding: var(--container-padding);
        display: flex;
        flex-direction: column;
        border-bottom: var(--container-darkest-border);
    }
    .panel-content-item {
        padding: var(--container-padding);
        display: flex;
        flex-direction: row;
        align-items: center;
        height: var(--line-height);
    }

    .panel-label {
        width: 110px;
    }

    .panel-value {
        width: 100px;
        justify-content: center;
        display: flex;
        justify-content: flex-end;
    }
</style>
