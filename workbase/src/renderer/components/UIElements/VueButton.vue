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
    props: ['text', 'intent', 'icon', 'active', 'alignText', 'disabled', 'fill', 'loading', 'rightIcon', 'small', 'className'],
    mounted() {
      this.$nextTick(() => {
        this.renderButton();
      });
    },
    watch: {
      className() {
        this.renderButton();
      },
      loading() {
        this.renderButton();
      },
      disabled() {
        this.renderButton();
      },
      icon() {
        this.renderButton();
      },
      text() {
        this.renderButton();
      },
    },
    methods: {
      renderButton() {
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
          small: this.small,
          className: this.className,
          onClick: () => { this.$emit('clicked'); },
        }), this.$refs.vueButton);
      },
    },
  };
</script>

