<template>
    <div ref="vueTabs"></div>
</template>

<style scoped>

</style>

<script>
  import { Tabs, Tab } from '@blueprintjs/core';

  import ReactDom from 'react-dom';
  import React from 'react';

  export default {
    name: 'VueTabs',
    props: ['animate', 'defaultSelectedTabId', 'id', 'selectedTabId', 'vertical', 'tabs'],
    data() {
      return {
        tabsElement: null,
      };
    },
    watch: {
      tabs() {
        this.renderTabs();
        ReactDom.render(this.tabsElement, this.$refs.vueTabs);
      },
    },
    created() {
      this.renderTabs();
    },
    mounted() {
      this.$nextTick(() => {
        ReactDom.render(this.tabsElement, this.$refs.vueTabs);
      });
    },
    methods: {
      renderTabs() {
        const tabElements = this.tabs.map(x => React.createElement(Tab, {
          className: 'vue-tab',
          id: x,
          title: x,
          key: x,
        }));

        this.tabsElement = React.createElement(Tabs, {
          className: 'vue-tabs',
          animate: this.animate,
          defaultSelectedTabId: this.defaultSelectedTabId,
          id: this.id,
          selectedTabId: this.selectedTabId,
          vertical: this.vertical,
          onChange: (tab) => { this.$emit('tab-selected', tab); },
        }, tabElements);
      },
    },
  };
</script>

