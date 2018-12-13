<template>
    <div v-show="contextMenu.show" ref="contextMenu" id="context-menu" class="z-depth-2">
        <li @click="(enableDelete) ? deleteNode() : false" class="context-action delete-nodes" :class="{'disabled':!enableDelete}">Delete</li>
    </div>
</template>
<script>
  import { DELETE_SCHEMA_CONCEPT } from '@/components/shared/StoresActions';
  import { createNamespacedHelpers } from 'vuex';


  export default {
    name: 'ContextMenu',
    props: [],
    beforeCreate() {
      const { mapGetters, mapMutations, mapActions } = createNamespacedHelpers('schema-design');

      // computed
      this.$options.computed = {
        ...(this.$options.computed || {}),
        ...mapGetters(['contextMenu', 'selectedNodes']),
      };

      // methods
      this.$options.methods = {
        ...(this.$options.methods || {}),
        ...mapMutations(['setContextMenu']),
        ...mapActions([DELETE_SCHEMA_CONCEPT]),
      };
    },
    computed: {
      enableDelete() {
        return (this.selectedNodes);
      },
    },
    watch: {
      contextMenu(contextMenu) {
        this.$refs.contextMenu.style.left = `${contextMenu.x}px`;
        this.$refs.contextMenu.style.top = `${contextMenu.y}px`;
      },
    },
    methods: {
      async deleteNode() {
        this.setContextMenu({ show: false, x: null, y: null });

        const label = this.selectedNodes[0].label;

        this[DELETE_SCHEMA_CONCEPT](this.selectedNodes[0])
          .then(() => {
            this.$notifyInfo(`Schema concept, ${label}, has been deleted`);
          })
          .catch((err) => { this.$notifyError(err, 'Delete schema concept'); });
      },
    },
  };
</script>
<style>
    #context-menu{
        position: absolute;
        background-color: #282828;
        padding: 8px 0px;
        z-index: 10;
        min-width: 100px;
        border: var(--container-darkest-border);
    }

    .context-action.disabled{
        opacity: 0.2;
        cursor: default;
    }

    .context-action{
        padding:8px 10px;
        cursor: pointer;
        opacity: 0.8;
        list-style-type: none;
    }

    .context-action:not(.disabled):hover{
        opacity: 1;
        background-color: #404040;
    }
</style>


