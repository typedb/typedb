<template>
    <div ref="vueSwitch"></div>
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
        this.renderSwitch();
      });
    },
    methods: {
      renderSwitch() {
        ReactDom.render(React.createElement(Switch, {
          className: 'vue-switch',
          alignIndicator: this.alignIndicator,
          checked: this.checked,
          defaultChecked: this.defaultChecked,
          disabled: this.disabled,
          inline: this.inline,
          label: this.label,
          onChange: (x) => {
            this.$emit('switch-changed', x.target.checked);
          },
        }), this.$refs.vueSwitch);
      },
    },
  };
</script>

