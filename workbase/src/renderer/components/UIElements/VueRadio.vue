<template>
    <div ref="vueRadio"></div>
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
        ReactDom.render(React.createElement(Radio, {
          className: 'vue-radio',
          alignIndicator: this.alignIndicator,
          checked: this.checked,
          defaultChecked: this.defaultChecked,
          disabled: this.disabled,
          inline: this.inline,
          label: this.label,
        }), this.$refs.vueRadio);
      });
    },
  };
</script>

