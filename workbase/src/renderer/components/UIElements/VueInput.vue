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
    props: ['text', 'intent', 'defaultValue', 'disabled', 'leftIcon', 'placeholder', 'rightElement', 'value'],
    mounted() {
      this.$nextTick(() => {
        ReactDom.render(React.createElement(InputGroup, {
          className: 'vue-input',
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
      });
    },
  };
</script>

