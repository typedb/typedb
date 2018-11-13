<template>
  <transition name="slide-fade" appear>
    <div class="design-wrapper">
      <top-bar></top-bar>
      <graph-canvas></graph-canvas>




        <!-- <menu-bar
            :localStore="localStore"
            @toggle-new-type-panel="toggleNewTypePanel"
            @toggle-attributes-panel="toggleAttributesPanel"
            @toggle-roles-panel="toggleRolesPanel"
        ></menu-bar>
        <div class="design-content">
                <new-type-panel :localStore="localStore" :showPanel="showNewTypePanel"></new-type-panel>
                <manage-schema-concepts
                    :instances="localStore.metaTypeInstances['roles']"
                    :showPanel="showRolesPanel"
                    concept="role"
                    @delete-schema-concept="deleteSchemaConcept"
                ></manage-schema-concepts>
                <manage-schema-concepts
                    :instances="localStore.metaTypeInstances['attributes']"
                    :showPanel="showAttributesPanel"
                    concept="attribute"
                    @delete-schema-concept="deleteSchemaConcept"
                ></manage-schema-concepts>
                <context-menu :localStore="localStore" @open-new-type-panel="showNewTypePanel=true"></context-menu>
                <graph-canvas :localStore="localStore"></graph-canvas>
                <right-side-bar :localStore="localStore"></right-side-bar>
                <Spinner className="spinner-schema" :localStore="localStore"></Spinner>
        </div> -->
    </div>
  </transition>
</template>

<style scoped>
.slide-fade-enter-active {
    transition: all .8s ease;
}
.slide-fade-enter,
.slide-fade-leave-active {
    opacity: 0;
}

.design-wrapper{
    display: flex;
    flex-direction: column;
    width: 100%;
    display: relative;
}

.design-content {
    display: flex;
    flex-direction: row;
    flex: 1;
    position: relative;
}

.content {
    display: flex;
    flex-direction: row;
    position: relative;
}

.left-side{
    position: absolute;
}

</style>

<script>
// import Spinner from '@/components/UIElements/Spinner.vue';
// import RightSideBar from '../RightSideBar/RightSidebar.vue';
// import MenuBar from './MenuBar.vue';
// import NewTypePanel from '../NewTypePanel/NewTypePanel.vue';
// import ManageSchemaConcepts from '../ManageSchemaConcepts';
// import ContextMenu from './ContextMenu.vue';
import GraphCanvas from '../shared/GraphCanvas.vue';
import TopBar from './TopBar';

// import localStore from '../SchemaDesignStore';
import { DELETE_SCHEMA_CONCEPT } from '../shared/StoresActions';

export default {
  name: 'SchemaDesignContent',
  components: {
    GraphCanvas, TopBar,
  },
  data() {
    return {
      showNewTypePanel: false,
      showAttributesPanel: false,
      showRolesPanel: false,
    };
  },
  created() {
    // this.localStore.registerCanvasEventHandler('click', this.closePanels);
  },
  methods: {
    deleteSchemaConcept(label) { this.localStore.dispatch(DELETE_SCHEMA_CONCEPT, label); },
    toggleNewTypePanel() {
      this.showNewTypePanel = !this.showNewTypePanel;
      if (this.showNewTypePanel) {
        this.showAttributesPanel = false;
        this.showRolesPanel = false;
      }
    },
    toggleAttributesPanel() {
      this.showAttributesPanel = !this.showAttributesPanel;
      if (this.showAttributesPanel) {
        this.showRolesPanel = false;
        this.showNewTypePanel = false;
      }
    },
    toggleRolesPanel() {
      this.showRolesPanel = !this.showRolesPanel;
      if (this.showRolesPanel) {
        this.showAttributesPanel = false;
        this.showNewTypePanel = false;
      }
    },
    closePanels() {
      this.showAttributesPanel = false;
      this.showNewTypePanel = false;
      this.showRolesPanel = false;
    },
  },
};
</script>
