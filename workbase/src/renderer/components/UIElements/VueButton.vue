<template>
    <div ref="vueButton"></div>
</template>

<style scoped>

</style>

<script>
  import { Button } from '@blueprintjs/core';

  import ReactDom from 'react-dom';
  import React from 'react';

  export default {
    name: 'VueButton',
    props: ['text', 'intent', 'icon', 'active', 'alignText', 'disabled', 'fill', 'loading', 'rightIcon'],
    mounted() {
      this.$nextTick(() => {
        ReactDom.render(React.createElement(Button, {
          text: this.text,
          intent: this.intent,
          icon: this.icon,
          active: this.active,
          alignText: this.alignText,
          disabled: this.disabled,
          fill: this.fill,
          loading: this.loading,
          rightIcon: this.rightIcon,
          className: 'vue-button',
        }), this.$refs.vueButton);
      });
    },
  };
</script>

