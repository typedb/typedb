<template>
<button v-if="editorLinesNumber>1" class="btn scroll-btn" @click="toggleEditorCollapse"><caret-icon class="scroll-caret" :toggleNorth="!isEditorCollapsed"></caret-icon></button>
</template>

<style scoped>

.scroll-btn {
    padding: 0px;
    margin: auto;
    height: 10px;
    width: 100%;
    display: inline-flex;
    border-radius: 0% 0% 5px 5px;
}

.scroll-caret {
  right: 45%;
  bottom: 7px;
}

</style>

<script>
import CaretIcon from '@/components/UIElements/CaretIcon.vue';
import $ from 'jquery';

export default {
  name: 'scrollButton',
  components: { CaretIcon },
  props: ['editorLinesNumber', 'codeMirror', 'toolTipShown'],
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
    toolTipShown(val) {
      console.log(val);
      if (val !== undefined) {
        $('.CodeMirror').animate({
          height: this.initialEditorHeight,
        }, 300);
        this.isEditorCollapsed = true;
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
      if (this.isEditorCollapsed) {
        $('.CodeMirror').animate({
          height: $('.CodeMirror-sizer').outerHeight(),
        }, 300, () => {
          $('.CodeMirror').css({
            height: 'auto',
          });
        });
        this.isEditorCollapsed = false;
      } else {
        $('.CodeMirror').animate({
          height: this.initialEditorHeight,
        }, 300);
        this.isEditorCollapsed = true;
      }
    },
  },
};
</script>
