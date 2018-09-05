<template>
    <div class="panel-container">
        <div @click="toggleContent" class="panel-header" v-bind:style="{ opacity: (this.nodes && this.nodes.length === 1) ? 1 : 0.5 }">
            <vue-icon :icon="(showAttributesPanel) ?  'chevron-down' : 'chevron-right'" iconSize="14" ></vue-icon>
            <h1>ATTRIBUTES</h1>
        </div>
        <div class="content" v-show="showAttributesPanel">
            <p v-if="!attributes.length">There are no attributes available for this type of node.</p>
            <div v-for="(value, key) in attributes" :key="key">
                <div class="content-item" v-if="value.href">
                    <div class="label">{{value.type}}:</div>
                    <a clas="value" :href="value.value" style="word-break: break-all; color:#00eca2;" target="_blank"> {{value.value}}</a>
                </div>
                <div class="content-item" v-else>
                    <div class="label">{{value.type}}:</div>
                    <div clas="value">{{value.value}}</div>
                </div>
            </div>
        </div>
    </div>
</template>

<script>
  export default {
    name: 'AttributesPanel',
    props: ['localStore'],
    data() {
      return {
        showAttributesPanel: false,
        attributes: [],
      };
    },
    computed: {
      nodes() {
        return this.localStore.getSelectedNodes();
      },
    },
    watch: {
      async nodes(nodes) {
        // If no node selected: close panel and return
        if (!nodes || nodes.length > 1) { this.showAttributesPanel = false; return; }

        const node = await this.localStore.getNode(nodes[0].id);

        const attributes = (node.isSchemaConcept()) ? [] : await Promise.all((await (await node.attributes()).collect()).map(async x => ({
          type: await (await x.type()).label(),
          value: await x.value(),
        })));
        this.attributes = Object.values(attributes).sort((a, b) => ((a.type > b.type) ? 1 : -1)).map(a => Object.assign(a, { href: this.validURL(a.value) }));

        this.showAttributesPanel = true;
      },
    },
    methods: {
      toggleContent() {
        if (this.nodes && this.nodes.length === 1) {
          this.showAttributesPanel = !this.showAttributesPanel;
        }
      },
      validURL(str) {
        const URL_REGEX = '^(?:(?:https?|ftp)://)(?:\\S+(?::\\S*)?@)?(?:' +
          '(?!(?:10|127)(?:\\.\\d{1,3}){3})' +
          '(?!(?:169\\.254|192\\.168)(?:\\.\\d{1,3}){2})' +
          '(?!172\\.(?:1[6-9]|2\\d|3[0-1])(?:\\.\\d{1,3}){2})' +
          '(?:[1-9]\\d?|1\\d\\d|2[01]\\d|22[0-3])' +
          '(?:\\.(?:1?\\d{1,2}|2[0-4]\\d|25[0-5])){2}' +
          '(?:\\.(?:[1-9]\\d?|1\\d\\d|2[0-4]\\d|25[0-4]))' +
          '|(?:(?:[a-z\\u00a1-\\uffff0-9]-*)*[a-z\\u00a1-\\uffff0-9]+)' +
          '(?:\\.(?:[a-z\\u00a1-\\uffff0-9]-*)*[a-z\\u00a1-\\uffff0-9]+)*' +
          '(?:\\.(?:[a-z\\u00a1-\\uffff]{2,}))\\.?)(?::\\d{2,5})?' +
          '(?:[/?#]\\S*)?$';

        const pattern = new RegExp(URL_REGEX, 'i');
        return pattern.test(str);
      },
    },
  };
</script>

<style scoped>

    .content {
        padding: var(--container-padding);
        display: flex;
        flex-direction: column;
        max-height: 280px;
    }

    .content-item {
        padding: var(--container-padding);
        display: flex;
        flex-direction: row;
    }

    .label {
        margin-right: 20px;
        width: 65px;
    }

</style>
