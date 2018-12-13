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
    props: ['text', 'icon', 'disabled', 'loading', 'rightIcon', 'className'],
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
          icon: this.icon,
          loading: this.loading,
          rightIcon: this.rightIcon,
          className: this.className,
          disabled: this.disabled,
          onClick: (e) => { this.$emit('clicked', e); },
        }), this.$refs.vueButton);
      },
    },
  };
</script>

