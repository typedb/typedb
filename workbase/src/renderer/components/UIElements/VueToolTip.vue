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
    props: ['autoFocus', 'content', 'defaultIsOpen', 'disabled', 'intent', 'isOpen', 'position', 'targetClassName', 'child', 'className', 'usePortal'],
    data() {
      return {
        clickEvent: () => {
          this.$emit('close-tooltip');
        },
      };
    },
    mounted() {
      this.$nextTick(() => {
        this.renderTooltip();
        if (this.isOpen) {
          window.addEventListener('click', this.clickEvent);
        } else {
          window.removeEventListener('click', this.clickEvent);
        }
      });
    },
    created() {

    },
    watch: {
      isOpen(val) {
        this.renderTooltip();

        if (val) {
          window.addEventListener('click', this.clickEvent);
        } else {
          window.removeEventListener('click', this.clickEvent);
        }
      },
      child() {
        this.renderTooltip();
      },
    },
    methods: {
      renderTooltip() {
        ReactDom.render(React.createElement(Tooltip, {
          className: this.className,
          autoFocus: this.autofocus,
          content: this.content,
          defaultIsOpen: this.defaultIsOpen,
          disabled: this.disabled,
          intent: this.intent,
          isOpen: this.isOpen,
          usePortal: this.usePortal,
          position: this.position,
          targetClassName: this.targetClassName,
        }, this.child), this.$refs.graknTooltip);
      },
    },
  };
</script>

