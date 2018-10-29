<template>
    <transition name="fade" appear>
        <div>

            <div class="vis-tabs">
              <div v-for="tab in Array.from(tabs.values())" :key="tab">
                <div :class="(tab === currentTab) ? 'tab current-tab' : 'tab'">
                  <div @click="toggleTab(tab)">Tab {{tab}}</div>
                  <div @click="closeTab(tab)"><vue-icon className="tab-icon" icon="cross" iconSize="13"></vue-icon></div>
                </div>
              </div>
              <button v-if="tabs.size < 11" @click="newTab" class='btn new-tab-btn'><vue-icon icon="plus" className="vue-icon"></vue-icon></button>
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

  .vis-tabs {
    position: absolute;
    bottom: 2.3%;
    z-index: 1;
    width: 100%;
    display: flex;
    align-items: center;
  }

  .tab {
    background-color: var(--gray-1);
    width: 120px;
    height: 30px;
    border: var(--container-darkest-border);
    display: flex;
    align-items: center;
    justify-content: center;
    cursor: pointer;
    justify-content: space-between;
    padding: var(--container-padding);
  }

  .current-tab {
    background-color: var(--gray-3);
  }

</style>

<script>

import VisTab from './VisTab.vue';

export default {
  name: 'VisualiserContent',
  components: { VisTab },
  data() {
    return {
      currentTab: 0,
      tabs: new Set([0]),
      visTab: 'VisTab',
    };
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

      if (this.currentTab === tab) this.currentTab = Array.from(this.tabs.values())[0];
      else {
        const temp = this.currentTab;
        this.currentTab = null;
        this.currentTab = temp;
      }
    },
  },
};
</script>
