<template>
    <div class="footer noselect" v-if="currentKeyspace">
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
        background-color: #282828;
        padding: var(--container-padding);
        display: flex;
        flex-direction: row;
        z-index: 1;
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
export default {
  name: 'GraknFooter',
  props: ['tabId'],
  computed: {
    currentKeyspace() {
      return this.$store.getters.currentKeyspace(this.tabId);
    },
    canvasData() {
      return this.$store.getters.canvasData(this.tabId);
    },
  },
};
</script>
