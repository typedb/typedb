<template>
    <transition name="fade" appear>
        <div>

            <div class="vis-tabs noselect">
              <div v-for="tab in Array.from(tabs.values())" :key="tab">
                <div :class="(tab === currentTab) ? 'tab current-tab' : 'tab'">
                  <div v-if="tabToRename !== tab" @click="toggleTab(tab)" @click.right="renameTab(tab)" class="tab-title">Tab {{tab}}</div>
                  <input v-else class="input-small rename-tab-input" :value="newName">
                  <div v-if="tabs.size > 1" @click="closeTab(tab)" class="close-tab-btn"><vue-icon className="tab-icon" icon="cross" iconSize="13"></vue-icon></div>
                  <div v-if="tabToRename === tab" @click="saveName(tab)" class="close-tab-btn"><vue-icon className="tab-icon" icon="tick" iconSize="13"></vue-icon></div>
                </div>
              </div>
              <button v-if="tabs.size < 10" @click="newTab" class='btn new-tab-btn'><vue-icon icon="plus" className="vue-icon"></vue-icon></button>
            </div>

            <keep-alive>
              <template v-for="tab in Array.from(tabs.values())">
                  <component v-if="currentTab === tab" :is="visTab" :tabId="tab" :key="tab"></component>
              </template>
            </keep-alive>

        </div>
    </transition>
</template>

<style scoped>

  .rename-tab-input {
    width: 60px;
  }

  .new-tab-btn {
    margin-left: 0px !important;
  }

  .tab-title {
    width: 100px;
    height: 100%;
    display: flex;
    align-items: center;
  }

  .vis-tabs {
    position: absolute;
    bottom: 22px;
    z-index: 1;
    width: 100%;
    display: flex;
    align-items: center;
    height: 30px;
    background-color: var(--gray-3);
  }

  .tab {
    background-color: var(--gray-1);
    width: 100px;
    height: 30px;
    border: var(--container-darkest-border);
    border-top: none;
    display: flex;
    align-items: center;
    justify-content: center;
    cursor: pointer;
    justify-content: space-between;
    padding-left: var(--container-padding);
    padding-right: var(--container-padding);
  }

  .current-tab {
    background-color: var(--canvas-color);
  }

</style>

<script>

import VisTab from './VisTab.vue';

export default {
  name: 'VisualiserContent',
  components: { VisTab },
  data() {
    return {
      currentTab: 1,
      tabs: new Set([1]),
      visTab: 'VisTab',
      LETTER_T_KEYCODE: 84,
      tabToRename: undefined,
      newName: '',
    };
  },
  created() {
    window.addEventListener('keydown', (e) => {
      if ((e.keyCode === this.LETTER_T_KEYCODE) && e.metaKey && this.tabs.size < 10) this.newTab();
    });
  },
  methods: {
    toggleTab(tab) {
      this.currentTab = tab;
    },
    newTab() {
      const newTabId = Math.max(...Array.from(this.tabs.values())) + 1;
      this.tabs.add(newTabId);
      this.currentTab = newTabId;
    },
    closeTab(tab) {
      this.$children.forEach((x) => {
        if (x.tabId && x.tabId === tab) x.$destroy();
      });
      this.tabs.delete(tab);

      if (this.currentTab === tab) this.currentTab = Array.from(this.tabs.values())[Math.max(...Array.from(this.tabs.values())) - 1];
      else {
        const temp = this.currentTab;
        this.currentTab = null;
        this.currentTab = temp;
      }
    },
    renameTab(tab) {
      this.tabToRename = tab;
    },
  },
};
</script>
