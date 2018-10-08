<template>
    <div ref="vueInput"></div>
</template>

<style scoped>

</style>

<script>
  import { InputGroup } from '@blueprintjs/core';

  import ReactDom from 'react-dom';
  import React from 'react';

  export default {
    name: 'VueInput',
    props: ['text', 'intent', 'defaultValue', 'disabled', 'leftIcon', 'placeholder', 'rightElement', 'className'],
    data() {
      return {
        value: undefined,
      };
    },
    mounted() {
      this.$nextTick(() => {
        this.renderQueryNameInput();
      });
    },
    watch: {
      defaultValue() {
        this.renderQueryNameInput();
      },
    },
    methods: {
      renderQueryNameInput() {
        ReactDom.render(React.createElement(InputGroup, {
          className: this.className,
          defaultValue: this.defaultValue,
          disabled: this.disabled,
          intent: this.intent,
          leftIcon: this.leftIcon,
          placeholder: this.placeholder,
          rightElement: this.rightElement,
          value: this.value,
          type: 'text',
          onChange: (val) => { this.$emit('input-changed', val.target.value); },
        }), this.$refs.vueInput);
      },
    },
  };
</script>

