<template>
    <div id="vue-tab"></div>
</template>

<style scoped>

</style>

<script>
  import { Tab } from '@blueprintjs/core';

  import ReactDom from 'react-dom';
  import React from 'react';

  export default {
    name: 'VueTab',
    props: ['animate', 'defaultSelectedTabId', 'id', 'selectedTabId', 'vertical'],
    mounted() {
      this.$nextTick(() => {
        const TabElement = document.getElementById('vue-tab');

        ReactDom.render(React.createElement(Tab, {
          className: 'vue-tab',
          children: this.children,
          disabled: this.disabled,
          id: this.id,
          panel: this.panel,
          title: this.title,
        }), TabElement);
      });
    },
  };
</script>

