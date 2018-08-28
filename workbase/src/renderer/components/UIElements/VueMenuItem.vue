<template>
    <div ref="vueMenuItem"></div>
</template>

<style scoped>

</style>

<script>
  import { MenuItem } from '@blueprintjs/core';

  import ReactDom from 'react-dom';
  import React from 'react';

  export default {
    name: 'VueMenuItem',
    props: ['active', 'children', 'disabled', 'icon', 'intent', 'label', 'text'],
    mounted() {
      this.$nextTick(() => {
        ReactDom.render(React.createElement(MenuItem, {
          className: 'vue-menu-item',
          active: this.active,
          children: this.children,
          disabled: this.disabled,
          icon: this.icon,
          intent: this.intent,
          label: this.label,
          text: this.text,
        }), this.$refs.vueMenuItem);
      });
    },
  };
</script>

