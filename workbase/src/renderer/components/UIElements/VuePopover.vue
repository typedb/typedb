<template>
    <div id="vue-popover"></div>
</template>

<style scoped>

</style>

<script>
  import { Popover } from '@blueprintjs/core';

  import ReactDom from 'react-dom';
  import React from 'react';

  export default {
    name: 'VuePopover',
    props: [
      'autoFocus',
      'canEscapeKeyClose',
      'content',
      'disabled',
      'hasBackdrop',
      'isOpen',
      'popoverClassName',
      'position',
      'target',
      'targetClassName',
      'transitionDuration'],

    mounted() {
      this.$nextTick(() => {
        const popoverElement = document.getElementById('vue-popover');

        ReactDom.render(React.createElement(Popover, {
          className: 'vue-popover',
          autoFocus: this.autoFocus,
          canEscapeKeyClose: this.canEscapeKeyClose,
          content: this.content,
          disabled: this.disabled,
          hasBackdrop: this.hasBackdrop,
          isOpen: this.isOpen,
          popoverClassName: this.popoverClassName,
          position: this.position,
          target: this.content,
          targetClassName: this.targetClassName,
          transitionDuration: this.transitionDuration,
        }), popoverElement);
      });
    },
  };
</script>

