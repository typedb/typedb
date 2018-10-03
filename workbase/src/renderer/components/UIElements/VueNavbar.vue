<template>
    <div id="vue-navbar"></div>
</template>

<style scoped>

</style>

<script>
  import { Navbar } from '@blueprintjs/core';

  import ReactDom from 'react-dom';
  import React from 'react';

  export default {
    name: 'VueNavbar',
    props: ['fixedToTop', 'align'],
    mounted() {
      this.$nextTick(() => {
        const navbarElement = document.getElementById('vue-navbar');

        ReactDom.render(React.createElement(Navbar, {
          className: 'vue-navbar',
          fixedToTop: this.fixedToTop,
          align: this.align,
        }), navbarElement);
      });
    },
  };
</script>

