<template>
    <div id="vue-radio"></div>
</template>

<style scoped>

</style>

<script>
  import { Radio } from '@blueprintjs/core';

  import ReactDom from 'react-dom';
  import React from 'react';

  export default {
    name: 'VueRadio',
    props: ['alignIndicator', 'checked', 'defaultChecked', 'disabled', 'inline', 'label'],
    mounted() {
      this.$nextTick(() => {
        const radioElement = document.getElementById('vue-radio');

        ReactDom.render(React.createElement(Radio, {
          className: 'vue-radio',
          alignIndicator: this.alignIndicator,
          checked: this.checked,
          defaultChecked: this.defaultChecked,
          disabled: this.disabled,
          inline: this.inline,
          label: this.label,
        }), radioElement);
      });
    },
  };
</script>

