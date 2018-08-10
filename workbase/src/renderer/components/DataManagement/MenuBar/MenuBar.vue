<template>
  <transition name="slide-down" appear>
    <nav role="navigation" class="navbar-fixed noselect">
      <div class="nav-wrapper">
        <div class="left">
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
          <query-settings class="query-settings" 
            :currentKeyspace="currentKeyspace" 
            :toolTipShown="toolTipShown" 
            v-on:toggle-tool-tip="toggleToolTip">
          </query-settings>
        </div>
        <div class="right">
          <keyspaces-handler 
            :localStore="localStore" 
            :toolTipShown="toolTipShown" 
            v-on:toggle-tool-tip="toggleToolTip">
          </keyspaces-handler>
        </div>
      </div>
    </nav>
  </transition>
</template>

<style scoped>

.menu-item{
    cursor: pointer;
}

.disabled{
    opacity:0.5;
    cursor: default;
}

.line{
    display: flex;
    flex-direction: row;
}

.search{
    margin-right: 8px;
}

.img-button{
    margin: 0 8px;
}


.nav-wrapper {
    display: flex;
    padding: 5px;
    align-items: center;
    border-bottom: 1px solid #29292B;
}

.right{
    display: flex;
    justify-content: flex-end;
    align-items: center;
    flex: 1;
}

.left{
    display: flex;
    justify-content: flex-end;
    align-items: center;
    flex: 1;
}


.center{
    display: flex;
    align-items: center;
    flex: 3;
}

.navbar-fixed {
    position: relative;
    z-index: 2;
    min-height: 22px;
    width: 100%;
}

.dark .navbar-fixed{
    background-color: #1A1A1A;
}

.light .navbar-fixed{
    background-color:#f2f4f7;
}

</style>

<script>
import KeyspacesHandler from '../../shared/KeyspacesHandler/KeyspacesHandler.vue';
import GraqlEditor from './GraqlEditor/GraqlEditor.vue';
import FavQueriesList from './FavQueries/FavQueriesList.vue';
import TypesPanel from './TypesPanel.vue';
import QuerySettings from './QuerySettings/QuerySettings.vue';
import FavQueriesSettings from './FavQueries/FavQueriesSettings';

export default {
  props: ['localStore'],
  components: { KeyspacesHandler, GraqlEditor, FavQueriesList, TypesPanel, QuerySettings },
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
