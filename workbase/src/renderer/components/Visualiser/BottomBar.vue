<template>
    <div class="bottom-bar-wrapper">
        <div class="bottom-bar-nav">
            <div class="bottom-bar-arrow" @click="toggleBottomBar"><vue-icon :icon="(showBottomBarContent) ? 'chevron-down' : 'chevron-up'"></vue-icon></div>

        </div>

        <div v-if="showBottomBarContent" class="content">
            <textarea class="graql-editor" id="graqlEditor" ref="consoleEditor" rows="3" placeholder=">>"></textarea>
            <ul id="output">

            </ul>
        </div>
    </div>
</template>

<style scoped>

.bottom-bar-wrapper {
  background-color: var(--gray-1);
  border-top: var(--container-darkest-border);
  width: 100%;
  position: relative;
}

.bottom-bar-nav{
  border-bottom: var(--container-darkest-border);
  width: 100%;
  height: 20px;
  flex-direction: row;
}

.bottom-bar-arrow {
  height: 100%;
  position: relative;
  cursor: pointer;
  float: right;
  justify-content: center;
  display: flex;
  align-items: center;
}

.content {
    height: 150px;
    display: flex;
    flex-direction: column-reverse;
    background-color: #100F0F;
}

#output {
    display: flex;
    flex-direction: column;
    overflow: auto;
    height: 100%;
}

</style>

<script>
import GraqlCodeMirror from './TopBar/GraqlEditor/GraqlCodeMirror';
import ConsoleUtils from './BottomBar/ConsoleUtils';

export default {
  data() {
    return {
      showBottomBarContent: false,
      codeMirror: {},
      currentQuery: '',
      scrolled: false,
    };
  },
  watch: {
    showBottomBarContent(val) {
      this.$nextTick(() => {
        if (val) {
          this.codeMirror = GraqlCodeMirror.getCodeMirror(this.$refs.consoleEditor);
          this.codeMirror.setOption('extraKeys', {
            Enter: this.runConsoleQuery,
            'Shift-Enter': 'newlineAndIndent',
          });
        }
      });
    },
  },
  methods: {
    toggleBottomBar() {
      this.showBottomBarContent = !this.showBottomBarContent;
    },
    async runConsoleQuery() {
      const result = await this.$store.dispatch('exectueQuery', { query: this.codeMirror.getValue() });

      result.forEach(async (x) => {
        const output = await ConsoleUtils.conceptToString(x);
        const item = document.createElement('LI');
        const textnode = document.createTextNode(output);
        item.appendChild(textnode);
        document.getElementById('output').appendChild(item);
        this.updateScroll();
      });
    },
    updateScroll() {
      if (!this.scrolled) {
        const element = document.getElementById('output');
        element.scrollTop = element.scrollHeight;
      }
    },
  },
};
</script>
