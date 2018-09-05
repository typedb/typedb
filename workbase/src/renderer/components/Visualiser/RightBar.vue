<template>
    <!--<vue-draggable-resizable :w="350" :minw="200" :draggable="false" class="right-bar-container" axis="x" :handles="['ml']" :parent="true">-->
    <div class="right-bar-container">
        <div class="nav">
            <!--<div @click="toggleNodeTab" class="nav-tab"><vue-button icon="search-around"  :className="(showNodeTab) ? 'nav-btn-selected' : 'nav-btn'"></vue-button></div>-->
            <!--<div @click="toggleSettingsTab" class="nav-tab"><vue-button id="cog" class="rotate" icon="cog" :className="(showSettingsTab) ? 'nav-btn-selected' : 'nav-btn'"></vue-button></div>-->
            <div @click="toggleNodeTab" :class="(showNodeTab) ? 'nav-tab-selected' : 'nav-tab'" class="nav-tab"><vue-icon icon="search-around" id="node" class="rotate"></vue-icon></div>

            <div @click="toggleSettingsTab" :class="(showSettingsTab) ? 'nav-tab-selected' : 'nav-tab'" class="nav-tab"><vue-icon icon="cog" id="cog" class="rotate"></vue-icon></div>
        </div>
        <div class="content">
            <node-tab v-if="showNodeTab" :localStore="localStore"></node-tab>
            <settings-tab v-if="showSettingsTab" :localStore="localStore"></settings-tab>
        </div>


    </div>
    <!--</vue-draggable-resizable>-->
</template>

<style lang="scss">

    .rotate{
        -moz-transition: all 0.4s linear;
        -webkit-transition: all 0.4s linear;
        transition: all 0.4s linear;
    }

    .down{
        -ms-transform: rotate(90deg);
        -moz-transform: rotate(90deg);
        -webkit-transform: rotate(90deg);
        transform: rotate(90deg);
    }

    .handle-ml {
        height: 100% !important;
        top: 0% !important;
        background: none !important;
        border: none !important;
    }

    .right-bar-container {
        background-color: var(--gray-1);
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
        border-bottom: var(--container-darkest-border);
    }

    .nav-tab {
        background-color: var(--gray-2);
        width: 30px;
        display: flex;
        align-items: center;
        justify-content: center;
        cursor: pointer;
    }

    .nav-tab:hover {
        border: 1px solid var(--button-hover-border-color) !important;

    }

    .nav-tab:active {
        border: 1px solid var(--button-active-border-color) !important;
    }

    .nav-tab-selected {
        background-color: var(--gray-3);
        width: 30px;
        height: 31px;
        display: flex;
        align-items: center;
        justify-content: center;
        cursor: pointer;
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
        document.getElementById('node').classList.toggle('down');
      },
      toggleSettingsTab() {
        this.showSettingsTab = true;
        this.showNodeTab = false;
        document.getElementById('cog').classList.toggle('down');
      },
    },
  };
</script>
