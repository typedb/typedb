<template>
    <div class="types-container">
        <div @click="$emit('close-types-panel')"><vue-icon class="close-container" icon="cross" iconSize="15" className="tab-icon"></vue-icon></div>

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
    </div>
</template>

<style scoped>

    .close-container{
        position: absolute;
        right: 0px;
        top:0px;
        height: 15px;
        z-index: 1;
    }

    .tab-panel {
    }

    .tabs {
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
        padding: var(--container-padding);
        border: var(--container-darkest-border);
        width: 100%;
        margin-top: 20px;
        height: 125px;
        overflow: auto;
        position: relative;
    }

    .column {
        display: flex;
        flex-direction: column;
    }

    .row {
        width: 100%;
        margin-top: 10px;
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
