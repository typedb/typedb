<template>
        <div class="types-container z-depth-3">
            <div class="column">
                <vue-tabs class="tabs" :tabs="tabs" v-on:tab-selected="toggleTab"></vue-tabs>
                <div class="row">
                    <div class="tab-panel" v-for="k in Object.keys(metaTypeInstances)" :key="k">
                        <div class="tab-list" v-show="currentTab===k">
                            <div v-for="i in metaTypeInstances[k]" :key="i">
                                <vue-button v-on:clicked="typeSelected(i)" :text="i" className="vue-button"></vue-button>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            <div class="editor-tab">
                <div @click="$emit('close-types-panel')"><vue-icon icon="cross" iconSize="12" className="tab-icon"></vue-icon></div>
            </div>
        </div>
</template>

<style scoped>

    .editor-tab {
        max-height: 123px;
        width: 13px;
        flex-direction: column;
        display: flex;
        position: relative;
        float: right;
        border-left: var(--container-light-border);
    }

    .tab-list {
        overflow: auto;
        height: 70px;
        display: flex;
        flex-wrap: wrap;
        flex-direction: row;
    }

    .types-container{
        background-color: var(--gray-2);
        border: var(--container-darkest-border);
        width: 100%;
        margin-top: 10px;
        max-height: 125px;
        position: relative;
        display: flex;
        flex-direction: row;
    }

    .column {
        display: flex;
        flex-direction: column;
        width: 100%;
        overflow-y: auto;
    }

    .column::-webkit-scrollbar {
        width: 1px;
    }

    .column::-webkit-scrollbar-thumb {
        background: var(--green-4);
    }

    .row {
        margin: var(--element-margin);
    }
</style>

<script>

  export default {
    name: 'TypesContainer',
    props: ['localStore', 'currentKeyspace'],
    data() {
      return {
        tabs: ['entities', 'attributes', 'relationships'],
        currentTab: 'entities',
      };
    },
    computed: {
      metaTypeInstances() {
        return this.localStore.getMetaTypeInstances();
      },
    },
    methods: {
      toggleTab(tab) {
        this.currentTab = tab;
      },
      typeSelected(type) {
        this.localStore.setCurrentQuery(`match $x isa ${type}; get;`);
      },
    },
  };
</script>
