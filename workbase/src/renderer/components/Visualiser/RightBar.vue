<template>
    <!--<vue-draggable-resizable :w="350" :minw="200" :draggable="false" class="right-bar-container" axis="x" :handles="['ml']" :parent="true">-->
    <div class="right-bar-container">
        <div class="nav">
            <div @click="toggleNodeTab" :class="(showNodeTab) ? 'nav-tab-selected' : 'nav-tab'" class="nav-tab"><vue-icon icon="info-sign"></vue-icon></div>
            <div @click="toggleSettingsTab" :class="(showSettingsTab) ? 'nav-tab-selected' : 'nav-tab'" class="nav-tab"><vue-icon icon="cog"></vue-icon></div>
            <div class="nav-bar-space"></div>
        </div>
        <div class="content">
            <node-tab v-if="showNodeTab" :localStore="localStore"></node-tab>
            <settings-tab v-if="showSettingsTab" :localStore="localStore"></settings-tab>
        </div>


    </div>
    <!--</vue-draggable-resizable>-->
</template>

<style lang="scss">

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
        background-color: var(--gray-2);
        border-left: var(--container-darkest-border);
        height: 100% !important;
        position: relative !important;
        left: 0px !important;
        word-wrap: break-word;
        width: 250px;
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
        width: 30px;
        display: flex;
        align-items: center;
        justify-content: center;
        cursor: pointer;
        border-bottom: 1px solid var(--gray-1);

    }

</style>

<script>
  import VueDraggableResizable from 'vue-draggable-resizable';
  import NodeTab from './RightBar/NodeTab';
  import SettingsTab from './RightBar/SettingsTab';

  export default {
    components: { VueDraggableResizable, NodeTab, SettingsTab },
    props: ['localStore'],
    data() {
      return {
        showNodeTab: true,
        showSettingsTab: false,
      };
    },
    methods: {
      toggleNodeTab() {
        this.showNodeTab = true;
        this.showSettingsTab = false;
      },
      toggleSettingsTab() {
        this.showSettingsTab = true;
        this.showNodeTab = false;
      },
    },
  };
</script>
