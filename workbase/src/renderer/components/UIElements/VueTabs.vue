<template>
    <div id="vue-tabs"></div>
</template>

<style scoped>

</style>

<script>
  import { Tabs } from '@blueprintjs/core';

  import ReactDom from 'react-dom';
  import React from 'react';

  export default {
    name: 'VueTabs',
    props: ['animate', 'defaultSelectedTabId', 'id', 'selectedTabId', 'vertical'],
    mounted() {
      this.$nextTick(() => {
        const TabsElement = document.getElementById('vue-tabs');

        ReactDom.render(React.createElement(Tabs, {
          className: 'vue-tabs',
          animate: this.animate,
          defaultSelectedTabId: this.defaultSelectedTabId,
          id: this.id,
          selectedTabId: this.selectedTabId,
          vertical: this.vertical,
        }), TabsElement);
      });
    },
  };
</script>

