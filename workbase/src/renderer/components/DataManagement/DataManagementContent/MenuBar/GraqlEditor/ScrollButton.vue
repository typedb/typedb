<template>
<span v-if="editorLinesNumber>1" @click="toggleEditorCollapse"><i :class="[isEditorCollapsed ? 'pe-7s-angle-down-circle' : 'pe-7s-angle-up-circle']"></i></span>
</template>

<style scoped>
span {
    color: #56C0E0;
    font-size: 25px;
    cursor: pointer;
    margin: auto;
    display: inline-flex;
}
</style>

<script>
import $ from 'jquery';

export default {
  name: 'scrollButton',
  props: ['editorLinesNumber', 'codeMirror'],
  data() {
    return {
      initialEditorHeight: undefined,
      isEditorCollapsed: false,
    };
  },

  created() {},
  watch: {
    editorLinesNumber(newVal) {
      // Set auto height when going back to 1 line and reset the boolean
      if (newVal === 1) {
        $('.CodeMirror').css({
          height: 'auto',
        });
        this.isEditorCollapsed = false;
      }
    },
  },
  mounted() {
    this.$nextTick(function mountedScrollButton() {
      $(document).ready(() => {
        this.initialEditorHeight = $('.CodeMirror').height();
        this.codeMirror.on('focus', () => {
          if (this.isEditorCollapsed) {
            $('.CodeMirror').animate({
              height: $('.CodeMirror-sizer').outerHeight(),
            }, 300, () => {
              $('.CodeMirror').css({
                height: 'auto',
              });
            });
            this.isEditorCollapsed = false;
          }
        });
      });
    });
  },

  methods: {
    toggleEditorCollapse() {
      if (!this.isEditorCollapsed) {
        $('.CodeMirror').animate({
          height: this.initialEditorHeight,
        }, 300);
        this.isEditorCollapsed = true;
      } else {
        $('.CodeMirror').animate({
          height: $('.CodeMirror-sizer').outerHeight(),
        }, 300, () => {
          $('.CodeMirror').css({
            height: 'auto',
          });
        });
        this.isEditorCollapsed = false;
      }
    },
  },
};
</script>
