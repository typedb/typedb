<template>
    <div id="vue-input"></div>
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
        const inputElement = document.getElementById('vue-input');

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
        }), inputElement);
      });
    },
  };
</script>

