<template>
    <div ref="vueIcon"></div>
</template>

<style scoped>

</style>

<script>
  import { Icon } from '@blueprintjs/core';

  import ReactDom from 'react-dom';
  import React from 'react';

  export default {
    name: 'VueIcon',
    props: ['color', 'icon', 'iconSize', 'intent', 'title', 'className'],
    mounted() {
      this.$nextTick(() => {
        this.renderIcon();
      });
    },
    watch: {
      icon() {
        this.renderIcon();
      },
    },
    methods: {
      renderIcon() {
        ReactDom.render(React.createElement(Icon, {
          className: this.className,
          color: this.color,
          icon: this.icon,
          iconSize: this.iconSize,
          intent: this.intent,
          title: this.title,
        }), this.$refs.vueIcon);
      },
    },
  };
</script>

