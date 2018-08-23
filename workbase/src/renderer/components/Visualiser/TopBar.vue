<template>
  <div class="top-bar-container">
    <div class="left">
      <img src="static/img/logo-text.png" class="grakn-icon">
      <h1>visualiser</h1>
    </div>
    <div class="center">
      <fav-queries-list 
        :currentKeyspace="currentKeyspace" 
        :favQueries="favQueries" 
        :toolTipShown="toolTipShown" 
        v-on:toggle-tool-tip="toggleToolTip" 
        ref="savedQueries">
      </fav-queries-list>
      <types-panel 
        :localStore="localStore" 
        :currentKeyspace="currentKeyspace" 
        :toolTipShown="toolTipShown" 
        v-on:type-selected="typeSelected" 
        v-on:meta-type-selected="metaTypeSelected"
        v-on:toggle-tool-tip="toggleToolTip"> 
      </types-panel>
      <graql-editor 
        :localStore="localStore" 
        v-on:refresh-fav-queries="refreshFavQueries" 
        :toolTipShown="toolTipShown" 
        v-on:toggle-tool-tip="toggleToolTip" 
        ref="graqlEditor">
      </graql-editor>
    </div>
    <div class="right">
      <keyspaces-handler 
        :localStore="localStore" 
        :toolTipShown="toolTipShown" 
        v-on:toggle-tool-tip="toggleToolTip">
      </keyspaces-handler>
    </div>
  </div>
</template>

<style lang="scss" scoped>
$width-left: 180px;
$width-right: 220px;
$min-width-center: 700px;

.top-bar-container {
  width: 100%;
  height: 50px;
  background-color: var(--light-color);
  position: relative;
  flex-direction: row;
  display: flex;
  justify-content: space-between;
  border-bottom: var(--container-border);
  }

.left {
  width: $width-left;
  height: 100%;
  display: flex;
  align-items: center;
  justify-content: space-between;
  // border-right: 1px solid white;
  padding: var(--container-padding);
}

.grakn-icon {
  height: 25px;
  margin-right: 5px;
}

.center {
  height: 100%;
  min-width: $min-width-center;
  display: flex;
  flex-direction: row;
  align-items: center;
  // justify-content: center;
}

.right {
  width: $width-right;
  height: 100%;
  display: flex;
  // border-left: 1px solid white;
  padding: var(--container-padding);
  justify-content: flex-end;
  align-items: center;
}
</style>

<script>
import KeyspacesHandler from '../shared/KeyspacesHandler/KeyspacesHandler.vue';
import GraqlEditor from './TopBar/GraqlEditor/GraqlEditor.vue';
import FavQueriesList from './TopBar/FavQueries/FavQueriesList.vue';
import TypesPanel from './TopBar/TypesPanel.vue';
import FavQueriesSettings from './TopBar/FavQueries/FavQueriesSettings';

export default {
  props: ['localStore'],
  components: { KeyspacesHandler, GraqlEditor, FavQueriesList, TypesPanel },
  data() {
    return {
      favQueries: [],
      toolTipShown: undefined,
    };
  },
  mounted() {
    this.$nextTick(() => {
      this.$refs.savedQueries.$on('type-fav-query', (favQuery) => {
        this.localStore.setCurrentQuery(favQuery);
      });

      this.$refs.savedQueries.$on('remove-fav-query', (queryName) => {
        FavQueriesSettings.removeFavQuery(queryName, this.currentKeyspace);
      });
    });
  },
  computed: {
    currentKeyspace() {
      return this.localStore.getCurrentKeyspace();
    },
    selectedNode() {
      return this.localStore.getSelectedNodes();
    },
  },
  watch: {
    currentKeyspace() {
      this.refreshFavQueries();
    },
    selectedNode() {
      if (this.selectedNode) {
        this.toolTipShown = undefined;
      }
    },
  },
  methods: {
    // Used by TypesPanel when clicking on rectangle containing a meta type
    typeSelected(type) {
      this.$refs.graqlEditor.$emit('type-selected', type);
      this.toggleToolTip();
    },
    // Used by TypesPanel when clicking on types or all types
    metaTypeSelected(metaType) {
      this.$refs.graqlEditor.$emit('meta-type-selected', metaType);
      this.toggleToolTip();
    },
    refreshFavQueries() {
      this.favQueries = this.objectToArray(FavQueriesSettings.getFavQueries(this.currentKeyspace));
    },
    toggleToolTip(val) {
      if (this.toolTipShown === val) {
        this.toolTipShown = undefined;
      } else {
        this.toolTipShown = val;
        this.localStore.visFacade.container.visualiser.getNetwork().unselectAll();
        this.localStore.setSelectedNodes(null);
      }
    },
    objectToArray(object) {
      return Object.keys(object).map(key => ({ name: key, value: object[key].replace('\n', '') }));
    },
  },
};
</script>
