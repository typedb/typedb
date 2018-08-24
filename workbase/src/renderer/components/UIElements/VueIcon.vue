<template>
    <div id="vue-icon"></div>
</template>

<style scoped>

</style>

<script>
  import { Icon } from '@blueprintjs/core';

  import ReactDom from 'react-dom';
  import React from 'react';

  export default {
    name: 'VueIcon',
    props: ['color', 'icon', 'iconSize', 'style', 'intent', 'title'],
    mounted() {
      this.$nextTick(() => {
        const iconElement = document.getElementById('vue-icon');

        ReactDom.render(React.createElement(Icon, {
          className: 'vue-icon',
          color: this.color,
          icon: this.icon,
          iconSize: this.iconSize,
          style: this.style,
          intent: this.intent,
          title: this.title,
        }), iconElement);
      });
    },
  };
</script>

