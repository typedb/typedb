<template>
    <div ref="vuePopover"></div>
</template>

<style scoped>

</style>

<script>
  import { Menu, MenuItem, Popover, Position } from '@blueprintjs/core';

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
      'transitionDuration',
      'button',
      'items',
    ],
    data() {
      return {
        menuElement: null,
        popoverElement: null,
      };
    },
    watch: {
      items() {
        this.renderMenu();
        ReactDom.render(this.popoverElement, this.$refs.vuePopover);
      },
      button() {
        this.renderMenu();
        ReactDom.render(this.popoverElement, this.$refs.vuePopover);
      },
    },
    created() {
      if (this.items) this.renderMenu();
    },
    mounted() {
      this.$nextTick(() => {
        ReactDom.render(this.popoverElement, this.$refs.vuePopover);
      });
    },
    methods: {
      renderMenu() {
        const itemElements = this.items.map(x => React.createElement(MenuItem, {
          className: 'vue-menu-item',
          text: x,
          key: x,
          onClick: () => { this.$emit('emit-item', x); },
        }));
        this.menuElement = React.createElement(Menu, {
          className: 'vue-menu',
          large: this.large,
          ulRef: this.ulRef,
        }, itemElements);

        this.popoverElement = React.createElement(Popover, {
          className: 'vue-popover',
          autoFocus: this.autoFocus,
          canEscapeKeyClose: this.canEscapeKeyClose,
          content: this.menuElement,
          disabled: this.disabled,
          hasBackdrop: this.hasBackdrop,
          isOpen: this.isOpen,
          popoverClassName: this.popoverClassName,
          position: Position.BOTTOM_LEFT,
          target: this.button,
          targetClassName: this.targetClassName,
          transitionDuration: this.transitionDuration,
        });
      },
    },
  };
</script>

