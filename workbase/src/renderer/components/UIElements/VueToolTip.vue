<template>
    <div ref="graknTooltip"></div>
</template>

<style scoped>

</style>

<script>
  import { Tooltip } from '@blueprintjs/core';

  import ReactDom from 'react-dom';
  import React from 'react';

  export default {
    name: 'VueTooltip',
    props: ['autoFocus', 'content', 'defaultIsOpen', 'disabled', 'intent', 'isOpen', 'position', 'targetClassName'],
    mounted() {
      this.$nextTick(() => {
        ReactDom.render(React.createElement(Tooltip, {
          className: 'vue-tooltip',
          autoFocus: this.autofocus,
          content: this.content,
          defaultIsOpen: this.defaultIsOpen,
          disabled: this.disabled,
          intent: this.intent,
          isOpen: this.isOpen,
          position: this.position,
          targetClassName: this.targetClassName,
        }), this.$refs.graknTooltip);
      });
    },
  };
</script>

