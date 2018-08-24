<template>
    <div id="vue-menu-divider"></div>
</template>

<style scoped>

</style>

<script>
  import { MenuDivider } from '@blueprintjs/core';

  import ReactDom from 'react-dom';
  import React from 'react';

  export default {
    name: 'VueMenuDivider',
    props: ['children', 'title'],
    mounted() {
      this.$nextTick(() => {
        const menuDividerElement = document.getElementById('vue-menu-divider');

        ReactDom.render(React.createElement(MenuDivider, {
          className: 'vue-menu-divider',
          children: this.children,
          title: this.title,
        }), menuDividerElement);
      });
    },
  };
</script>

