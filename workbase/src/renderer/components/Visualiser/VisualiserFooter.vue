<template>
    <div class="footer noselect">
        <div class="canvas-data">
            <div class="data-item no-of-entities"> entities: {{canvasData.entities}}</div>
            <div class="data-item no-of-attributes"> attributes: {{canvasData.attributes}}</div>
            <div class="data-item no-of-relationships"> relationships: {{canvasData.relationships}}</div>
        </div>
    </div>
</template>
<style scoped>
    .footer {
        border-top: var(--container-darkest-border);
        background-color: var(--gray-1);
        padding: var(--container-padding);
        display: flex;
        flex-direction: row;
        z-index: 2;
        justify-content: flex-end;
    }

    .canvas-data {
        display: flex;
    }

    .data-item {
        justify-content: space-between;
        display: flex;
        margin-left: 5px;
    }
</style>

<script>
import { createNamespacedHelpers } from 'vuex';

export default {
  name: 'VisualiserFooter',
  props: ['tabId'],
  beforeCreate() {
    const { mapGetters } = createNamespacedHelpers(`tab-${this.$options.propsData.tabId}`);

    // computed
    this.$options.computed = {
      ...(this.$options.computed || {}),
      ...mapGetters(['canvasData']),
    };
  },
};
</script>
