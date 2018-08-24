<template>
    <div id="vue-menu"></div>
</template>

<style scoped>

</style>

<script>
  import { Menu } from '@blueprintjs/core';

  import ReactDom from 'react-dom';
  import React from 'react';

  export default {
    name: 'VueMenu',
    props: ['large', 'ulRef'],
    mounted() {
      this.$nextTick(() => {
        const menuElement = document.getElementById('vue-menu');

        ReactDom.render(React.createElement(Menu, {
          className: 'vue-menu',
          large: this.large,
          ulRef: this.ulRef,
        }), menuElement);
      });
    },
  };
</script>

