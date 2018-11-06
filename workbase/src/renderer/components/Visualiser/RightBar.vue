<template>
    <!--<vue-draggable-resizable :w="350" :minw="200" :draggable="false" class="right-bar-container" axis="x" :handles="['ml']" :parent="true">-->
    <div>
            <div class="right-bar-container">
                <div class="minimize-right-bar" v-bind:style= "[showRightBar ? {} : {'opacity': '1'}]" @click="toggleRightBar">
                    <vue-icon :icon="(showRightBar) ? 'double-chevron-right' : 'double-chevron-left'" iconSize="14" className="vue-icon"></vue-icon>
                </div>

                <div class="nav" v-if="showRightBar">
                    <div @click="toggleConceptInfoTab" :class="(showConceptInfoTab) ? 'nav-tab nav-tab-selected' : 'nav-tab'" class="concept-info-tab"><vue-icon icon="info-sign" className="right-bar-tab-icon"></vue-icon></div>
                    <div @click="toggleSettingsTab" :class="(showSettingsTab) ? 'nav-tab nav-tab-selected' : 'nav-tab'" class="settings-tab"><vue-icon icon="cog" className="right-bar-tab-icon"></vue-icon></div>
                    <div class="nav-bar-space"></div>
                </div>

                <div class="content" v-if="showRightBar">
                    <keep-alive>
                        <concept-info-tab :tabId="tabId" v-if="showConceptInfoTab"></concept-info-tab>
                        <settings-tab :tabId="tabId" v-if="showSettingsTab"></settings-tab>
                    </keep-alive>
                </div>
            </div>
    </div>
    <!--</vue-draggable-resizable>-->
</template>

<style lang="scss">


    .minimize-right-bar {
        background-color: var(--gray-1);
        border-right: var(--container-darkest-border);
        border-top: var(--container-darkest-border);
        border-bottom: var(--container-darkest-border);
        width: 18px;
        height: 30px;
        position: absolute;
        right: 100%;
        top: 50%;
        opacity: 0;
        display: flex;
        align-items: center;
        justify-content: center;
        cursor: pointer;
        z-index:1;
    }

    .right-bar-container:hover {
        .minimize-right-bar {
            opacity: 1;
        }
    }

    .nav-bar-space {
        border-bottom: var(--container-darkest-border);
        display: flex;
        flex: 1;
    }


    .handle-ml {
        height: 100% !important;
        top: 0% !important;
        background: none !important;
        border: none !important;
    }

    .right-bar-container {
        background-color: var(--gray-3);
        border-left: var(--container-darkest-border);
        height: 100% !important;
        word-wrap: break-word;
        /*width: 250px;*/
        position: absolute;
        right: 0px;
        top: 0px;
        z-index: 1;
    }

    .nav {
        background-color: var(--gray-2);
        height: 30px;
        display: flex;
        flex-direction: row;
    }

    .nav-tab {
        background-color: var(--gray-2);
        border-right: var(--container-darkest-border);
        width: 30px;
        display: flex;
        align-items: center;
        justify-content: center;
        cursor: pointer;
        border-bottom: var(--container-darkest-border);
    }

    .nav-tab-selected {
        background-color: var(--gray-1);
        border-right: var(--container-darkest-border);
        border-bottom: 1px solid var(--gray-1);
        width: 30px;
        display: flex;
        align-items: center;
        justify-content: center;
        cursor: pointer;

    }

</style>

<script>
  import VueDraggableResizable from 'vue-draggable-resizable';
  import ConceptInfoTab from './RightBar/ConceptInfoTab';
  import SettingsTab from './RightBar/SettingsTab';

  export default {
    components: { VueDraggableResizable, ConceptInfoTab, SettingsTab },
    props: ['tabId'],
    data() {
      return {
        showConceptInfoTab: true,
        showSettingsTab: false,
        showRightBar: true,
      };
    },
    methods: {
      toggleConceptInfoTab() {
        this.showConceptInfoTab = true;
        this.showSettingsTab = false;
      },
      toggleSettingsTab() {
        this.showSettingsTab = true;
        this.showConceptInfoTab = false;
      },
      toggleRightBar() {
        this.showRightBar = !this.showRightBar;
      },
    },
  };
</script>
