<template>
    <div id="vue-menu-context-menu"></div>
</template>

<style scoped>

</style>

<script>
  import { ContextMenu } from '@blueprintjs/core';

  import ReactDom from 'react-dom';
  import React from 'react';

  export default {
    name: 'VueContextMenu',
    props: ['children', 'title'],
    mounted() {
      this.$nextTick(() => {
        const contextMenuElement = document.getElementById('vue-context-menu');

        ReactDom.render(React.createElement(ContextMenu, {
          className: 'vue-context-menu',
          children: this.children,
          title: this.title,
        }), contextMenuElement);
      });
    },
  };
</script>

