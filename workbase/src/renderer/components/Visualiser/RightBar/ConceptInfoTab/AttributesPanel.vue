<template>
    <div class="panel-container">
        <div @click="toggleContent" class="panel-header">
            <vue-icon :icon="(showAttributesPanel) ?  'chevron-down' : 'chevron-right'" iconSize="14" className="vue-icon"></vue-icon>
            <h1>Attributes</h1>
        </div>
        <div v-show="showAttributesPanel">
            <div class="content noselect" v-if="msg">
                {{msg}}
            </div>
            <div class="content" v-else>
                <div v-for="(value, key) in attributes" :key="key">
                    <div class="content-item" v-if="value.href">
                        <div class="label">{{value.type}}:</div>
                        <a class="value" :href="value.value" style="word-break: break-all; color:#00eca2;" target="_blank">{{value.value}}</a>
                    </div>
                    <div class="content-item" v-else>
                        <div class="label">{{value.type}}:</div>
                        <div class="value">{{value.value}}</div>
                    </div>
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
        showAttributesPanel: undefined,
        attributes: null,
      };
    },
    computed: {
      selectedNodes() {
        return this.localStore.getSelectedNodes();
      },
      currentKeyspace() {
        return this.localStore.getCurrentKeyspace();
      },
      msg() {
        if (!this.currentKeyspace) return 'Please select a keyspace';
        else if (!this.selectedNodes || this.selectedNodes.length > 1) return 'Please select a node';
        else if (!this.attributes) return 'Attributes are being loaded';
        else if (!this.attributes.length) return 'There are no attributes available for this type of node';
        return null;
      },
    },
    watch: {
      selectedNodes(nodes) {
        // If no node selected: close panel and return
        if (!nodes || nodes.length > 1) { this.showAttributesPanel = false; return; }

        const attributes = nodes[0].attributes;

        this.attributes = Object.values(attributes).sort((a, b) => ((a.type > b.type) ? 1 : -1)).map(a => Object.assign(a, { href: this.validURL(a.value) }));

        this.showAttributesPanel = true;
      },
    },
    methods: {
      toggleContent() {
        this.showAttributesPanel = !this.showAttributesPanel;
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
        border-bottom: var(--container-darkest-border);
        display: flex;
        flex-direction: column;
        max-height: 691px;
        overflow: auto;
    }

    .content::-webkit-scrollbar {
        width: 2px;
    }

    .content::-webkit-scrollbar-thumb {
        background: var(--green-4);
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

    .value {
        width: 110px;
    }

</style>
