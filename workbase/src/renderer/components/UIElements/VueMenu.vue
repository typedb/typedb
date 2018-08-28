<template>
    <div id="test" ref="vueMenu"></div>
</template>

<script>
  import { Menu, MenuItem } from '@blueprintjs/core';

  import ReactDom from 'react-dom';
  import React from 'react';

  export default {
    name: 'VueMenu',
    props: ['large', 'ulRef'],
    mounted() {
      this.$nextTick(() => {
        const item = React.createElement(MenuItem, {
          className: 'vue-menu-item',
          text: 'test',
          intent: 'primary',
        });

        ReactDom.render(React.createElement(Menu, {
          className: 'vue-menu',
          large: this.large,
          ulRef: this.ulRef,
        }, item, item), this.$refs.vueMenu);
      });
    },
  };
</script>

