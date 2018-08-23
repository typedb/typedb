<template>
  <div>
    <button :class="{'disabled':currentKeyspace, 'btn-selected':toolTipShown === 'typesPanel'}" @click="togglePanel" class="btn top-bar-btn types-btn" id="types-panel">
        Types
        <!-- <img class="types-arrow" :src="(toolTipShown === 'typesPanel') ? 'static/img/icons/icon_up_arrow.svg' : 'static/img/icons/icon_down_arrow.svg'"> -->
    </button> 
    <transition name="slide-fade" appear> 
      <div class="types-wrapper">
      <div v-if="toolTipShown === 'typesPanel'" class="types-panel">
        <div class="tabs-row">
            <div class="inline-div">
              <button @click="$emit('meta-type-selected', 'entity')" class="btn norightmargin" id="entities">Entities</button>
              <button @click="updateCurrentTab('entities')" class="btn noleftmargin noselect" :class="{'btn-selected':currentTab === 'entities'}" id="list-entities"><caret-icon :toggleNorth="currentTab === 'entities'"></caret-icon></button>
            </div>
            <div class="inline-div">
              <button @click="$emit('meta-type-selected', 'attribute')" class="btn norightmargin" id="attributes">Attributes</button>
              <button @click="updateCurrentTab('attributes')" class="btn noleftmargin noselect" :class="{'btn-selected':currentTab === 'attributes'}" id="list-attributes"><caret-icon :toggleNorth="currentTab === 'attributes'"></caret-icon></button>
            </div>
            <div class="inline-div">
              <button @click="$emit('meta-type-selected', 'relationship')" class="btn norightmargin" id="relationships">Relationships</button>
              <button @click="updateCurrentTab('relationships')" class="btn noleftmargin noselect" :class="{'btn-selected':currentTab === 'relationships'}" id="list-relationships"><caret-icon :toggleNorth="currentTab === 'relationships'"></caret-icon></button>
            </div>
        </div>
        <transition name="slide-fade" v-for="k in Object.keys(metaTypeInstances)" :key="k">
          <div v-bind:id="k+'-tab'" class="tab-pane" v-show="currentTab===k">
              <div v-bind:class="k+'-group concepts-group'">
                <div v-for="i in metaTypeInstances[k]" :key="i">
                  <button @click="$emit('type-selected', i)" class="btn btn-link" v-bind:id="i+'-btn'">{{i}}</button>
                </div>
              </div>
          </div>
        </transition>
        </div>
      </div>
    </transition>
  </div>
</template>

<script>
import CaretIcon from '@/components/UIElements/CaretIcon.vue';

export default {
  name: 'TypeInstacesPanel',
  props: ['localStore', 'currentKeyspace', 'toolTipShown'],
  components: { CaretIcon },
  data() {
    return {
      currentTab: undefined,
    };
  },
  computed: {
    metaTypeInstances() {
      return this.localStore.getMetaTypeInstances();
    },
  },
  methods: {
    togglePanel() {
      if (!(this.toolTipShown === 'typesPanel')) {
        this.$emit('toggle-tool-tip', 'typesPanel');
      } else {
        this.$emit('toggle-tool-tip');
        this.currentTab = undefined;
      }
    },
    updateCurrentTab(key) {
      if (this.currentTab === key) {
        this.currentTab = undefined;
      } else {
        this.currentTab = key;
      }
    },
  },
};
</script>

<style scoped>
a,
button {
    cursor: pointer;
}

.norightmargin{
  margin-right: 0px;
  border-top-right-radius: 0px;
  border-bottom-right-radius: 0px;
  border-right: 1px solid #606060;
}
.noleftmargin{
  margin-left: 0px;
  border-top-left-radius: 0px;
  border-bottom-left-radius: 0px;
  border-left: 1px solid #606060;
  padding-right: 3px;
}

.disabled{
    opacity:0.5;
    cursor: default;
}

.inline-div{
  display: inline-flex;
}

.types-wrapper {
  position: absolute;
}

.types-panel {
    position: relative;
    top: 100%;
    display: flex;
    flex-direction: column;
    z-index: 0;
    left: 85px;
}



.types-arrow {
  height: 20px;
}

.tab-pane {
    margin-top: 5px;
    position: absolute;
    margin-top: 11.5%;
}

a:hover {
    color: #00eca2;
}

.tabs-row {
    display: flex;
    flex-direction: row;
    justify-content: start;
}

.concepts-group {
    display: flex;
    flex-direction: row;
    flex-wrap: wrap;
}

.btn-link {
  white-space: nowrap;
}

.nav-tabs {
    width: 100%;
    display: flex;
    justify-content: space-around;
    flex-direction: row;
    align-items: center;
    flex: 1;
}

.slide-fade-enter-active {
    transition: all .6s ease;
}
.slide-fade-leave-active {
    transition: all .3s cubic-bezier(1.0, 0.5, 0.8, 1.0);
}
.slide-fade-enter,
.slide-fade-leave-active {
    transform: translateY(-10px);
    opacity: 0;
}
</style>