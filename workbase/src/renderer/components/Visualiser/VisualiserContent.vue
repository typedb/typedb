<template>
    <transition name="fade" appear>
        <div>

            <div class="vis-tabs noselect">
              <div v-for="tab in Array.from(tabs.keys())" :key="tab">
                <div :class="(tab === currentTab) ? 'tab current-tab' : 'tab'">

                  <div class="tab-content" v-if="tabToRename !== tab">
                    <div @click="toggleTab(tab)" @dblclick="renameTab(tab)" class="tab-title">{{(tabs.get(tab)) ? tabs.get(tab) : `Tab ${tab}`}}</div>
                    <div v-if="tabs.size > 1" @click="closeTab(tab)" class="close-tab-btn"><vue-icon className="tab-icon" icon="cross" iconSize="13"></vue-icon></div>
                  </div>
                  
                  <div v-else class="tab-content">
                    <input ref="renameTabInput" class="input-small rename-tab-input" v-model="newTabName">
                    <div @click="cancelRename" class="close-tab-btn"><vue-icon className="tab-icon" icon="cross" iconSize="13"></vue-icon></div>
                    <div  @click="saveName(tab)" class="close-tab-btn"><vue-icon className="tab-icon" icon="tick" iconSize="13"></vue-icon></div>
                  </div>

                </div>
              </div>
              <button v-if="tabs.size < 10" @click="newTab" class='btn new-tab-btn'><vue-icon icon="plus" className="vue-icon"></vue-icon></button>
            </div>

              <template v-for="tab in Array.from(tabs.keys())">
                <keep-alive :key="tab">
                  <component v-if="currentTab === tab" :is="visTab" :tabId="tab" :key="tab"></component>
                </keep-alive>
              </template>

        </div>
    </transition>
</template>

<style scoped>

  .tab-content {
    display: flex;
    align-items: center;
  }

  .rename-tab-input {
    width: 60px;
  }

  .new-tab-btn {
    margin-left: 0px !important;
  }

  .tab-title {
    width: 75px;
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
      tabs: new Map([[1, undefined]]),
      visTab: 'VisTab',
      LETTER_T_KEYCODE: 84,
      tabToRename: undefined,
      newTabName: '',
    };
  },
  created() {
    window.addEventListener('keydown', (e) => {
      // pressing CMD + T will create a new tab
      if ((e.keyCode === this.LETTER_T_KEYCODE) && e.metaKey && this.tabs.size < 10) this.newTab();

      if (this.tabToRename && e.keyCode === 13) this.saveName(this.tabToRename); // Pressing enter will save tab name
      if (this.tabToRename && e.keyCode === 27) this.tabToRename = undefined; // Pressing escape will cancel renaming of tab
    });
  },
  methods: {
    truncate(name) {
      if (name && name.length > 10) return `${name.substring(0, 10)}...`;
      return name;
    },
    toggleTab(tab) {
      this.currentTab = tab;
      this.cancelRename();
    },
    newTab() {
      const newTabId = Math.max(...Array.from(this.tabs.keys())) + 1; // Get max tab id and increment it for new tab id
      this.tabs.set(newTabId, undefined);
      this.currentTab = newTabId;
    },
    closeTab(tab) {
      // Find tab compoenent which has same tabId as tab to be closed and destroy it manually
      this.$children.filter(x => (x.tabId && x.tabId === tab))[0].$destroy();

      this.tabs.delete(tab);

      // if tab being closes is same as current tab switch to first tab in tabs
      if (this.currentTab === tab) this.currentTab = Array.from(this.tabs.keys())[0];
      else { // re-set the same tab to trigger dynamic rerendering of list
        const temp = this.currentTab;
        this.currentTab = null;
        this.currentTab = temp;
      }
    },
    renameTab(tab) {
      this.tabToRename = tab;
      this.newTabName = this.tabs.get(tab);
      this.$nextTick(() => this.$refs.renameTabInput[0].focus());
    },
    saveName(tab) {
      this.tabs.set(tab, this.truncate(this.newTabName));
      this.cancelRename();
    },
    cancelRename() {
      this.tabToRename = undefined;
      this.newTabName = '';
    },
  },
};
</script>
