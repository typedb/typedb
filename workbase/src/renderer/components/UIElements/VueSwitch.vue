<template>
    <div id="vue-switch"></div>
</template>

<style scoped>

</style>

<script>
  import { Switch } from '@blueprintjs/core';

  import ReactDom from 'react-dom';
  import React from 'react';

  export default {
    name: 'VueSwitch',
    props: ['alignIndicator', 'checked', 'defaultChecked', 'disabled', 'inline', 'label'],
    mounted() {
      this.$nextTick(() => {
        const switchElement = document.getElementById('vue-switch');

        ReactDom.render(React.createElement(Switch, {
          className: 'vue-switch',
          alignIndicator: this.alignIndicator,
          checked: this.checked,
          defaultChecked: this.defaultChecked,
          disabled: this.disabled,
          inline: this.inline,
          label: this.label,
        }), switchElement);
      });
    },
  };
</script>

