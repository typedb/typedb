<template>
    <div class="tooltip-container noselect" v-if="isOpen" :class="arrowPosition">
        {{msg}}
    </div>

</template>

<script>
  export default {
    name: 'ToolTip',
    props: ['msg', 'isOpen', 'arrowPosition'],
    data() {
      return {
        clickEvent: () => {
          this.$emit('close-tooltip');
        },
      };
    },
    mounted() {
      this.$nextTick(() => {
        if (this.isOpen) {
          window.addEventListener('click', this.clickEvent);
        } else {
          window.removeEventListener('click', this.clickEvent);
        }
      });
    },
    watch: {
      isOpen(val) {
        if (val) {
          window.addEventListener('click', this.clickEvent);
        } else {
          window.removeEventListener('click', this.clickEvent);
        }
      },
    },
  };
</script>

<style scoped>

    .tooltip-container {
        background-color: var(--purple-3);
        padding: 10px;
        border: 1px solid var(--purple-1);
        position: absolute;
        text-align: center;
        z-index: 2;
    }

    /*top*/

    .top:after, .top:before {
        bottom: 100%;
        left: 50%;
        border: solid transparent;
        content: " ";
        height: 0;
        width: 0;
        position: absolute;
        pointer-events: none;
    }

    .top:after {
        border-bottom-color: var(--purple-3);
        border-width: 10px;
        margin-left: -10px;
    }
    .top:before {
        border-bottom-color: var(--purple-1);
        border-width: 11px;
        margin-left: -11px;
    }

    /*right*/

    .right:after, .right:before {
        left: 100%;
        top: 50%;
        border: solid transparent;
        content: " ";
        height: 0;
        width: 0;
        position: absolute;
        pointer-events: none;
    }

    .right:after {
        border-left-color: var(--purple-3);
        border-width: 10px;
        margin-top: -10px;
    }
    .right:before {
        border-left-color: var(--purple-1);
        border-width: 11px;
        margin-top: -11px;
    }

    /*bottom*/

    .bottom:after, .bottom:before {
        top: 108%;
        left: 50%;
        border: solid transparent;
        content: " ";
        height: 0;
        width: 0;
        position: absolute;
        pointer-events: none;
    }

    .bottom:after {
        border-top-color: var(--purple-3);
        border-width: 10px;
        margin-left: -10px;
    }
    .bottom:before {
        border-top-color: var(--purple-1);
        border-width: 11px;
        margin-left: -11px;
    }

    /*left*/

    .left:after, .left:before {
        right: 108%;
        top: 50%;
        border: solid transparent;
        content: " ";
        height: 0;
        width: 0;
        position: absolute;
        pointer-events: none;
    }

    .left:after {
        border-right-color: var(--purple-3);
        border-width: 10px;
        margin-top: -10px;
    }
    .left:before {
        border-right-color: var(--purple-1);
        border-width: 11px;
        margin-top: -11px;
    }

</style>
