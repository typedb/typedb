<template>
<div class="keyspaces-wrapper">

    <button :class="(this.showKeyspaceList) ? 'btn keyspaces keyspace-btn' : 'btn keyspaces'" @click="toggleKeyspaceList">
        {{currentKeyspace | truncate}}
        <vue-icon icon="database" className="vue-icon database-icon"></vue-icon>
    </button>

    <tool-tip class="keyspace-tooltip" msg="Please select a keyspace" :isOpen="showKeyspaceTooltip" arrowPosition="right"></tool-tip>


        <ul id="keyspaces-list" class="keyspaces-list arrow_box z-depth-1" v-if="showKeyspaceList">
            <div style="text-align:center;" v-if="allKeyspaces && !allKeyspaces.length">no existing keyspace</div>
            <li :id="ks" v-bind:class="(ks === currentKeyspace)? 'ks-key active noselect' : 'ks-key noselect'" v-for="ks in allKeyspaces" :key="ks" @click="setKeyspace(ks)">{{ks}}</li>
        </ul>
</div>
</template>

<style scoped>

    .keyspaces-wrapper {
      z-index: 3;
    }

    .keyspace-tooltip {
        right: 100px;
        top: 8px;
    }

    .keyspaces {
        display: flex;
    }

    .arrow_box {
        position: relative;
        background: var(--gray-1);
        border: var(--container-darkest-border);
    }
    .arrow_box:after, .arrow_box:before {
        bottom: 100%;
        left: 50%;
        border: solid transparent;
        content: " ";
        height: 0;
        width: 0;
        position: absolute;
        pointer-events: none;
    }

    .arrow_box:after {
        border-bottom-color: var(--gray-1);
        border-width: 10px;
        margin-left: -10px;
    }
    .arrow_box:before {
        border-bottom-color: var(--border-darkest-color);
        border-width: 11px;
        margin-left: -11px;
    }


.keyspaces-list {
    position: absolute;
    top: 100%;
    margin-top: 5px;
    /*padding: 5px 10px;*/
    right:5px;
    background-color: #282828;
    z-index: 3;
    min-width: 100px;
    max-width:  130px;
    word-break: break-word;
    background-color: var(--gray-1);
    border: var(--container-darkest-border);
}

/*dynamic class*/

    .ks-key {
        position: relative;
        cursor: pointer;
        padding: 5px;
        min-height: 22px;

    }

    .ks-key:hover {
        background-color: var(--purple-4);
    }

</style>

<script>
import { createNamespacedHelpers, mapGetters } from 'vuex';

import storage from '@/components/shared/PersistentStorage';

import { CURRENT_KEYSPACE_CHANGED } from './StoresActions';
import ToolTip from '../UIElements/ToolTip';

export default {
  name: 'KeyspacesList',
  props: ['tabId', 'showKeyspaceTooltip'],
  components: { ToolTip },
  data() {
    return {
      keyspaceItems: [],
      showKeyspaceList: false,
      clickEvent: () => {
        this.showKeyspaceList = false;
      },
    };
  },
  beforeCreate() {
    const { mapGetters, mapActions } = createNamespacedHelpers(`tab-${this.$options.propsData.tabId}`);

    // computed
    this.$options.computed = {
      ...(this.$options.computed || {}),
      ...mapGetters(['currentKeyspace']),
    };

    // methods
    this.$options.methods = {
      ...(this.$options.methods || {}),
      ...mapActions([CURRENT_KEYSPACE_CHANGED]),
    };
  },
  computed: {
    ...mapGetters(['allKeyspaces', 'isGraknRunning']),
  },
  filters: {
    truncate(ks) {
      if (!ks) return 'keyspace';
      if (ks.length > 15) return `${ks.substring(0, 15)}...`;
      return ks;
    },
  },
  watch: {
    allKeyspaces(val) {
      // If user deletes current keyspace from Keyspaces page, set new current keyspace to null
      if (!val.includes(this.currentKeyspace)) { this[CURRENT_KEYSPACE_CHANGED](null); }
    },
    isGraknRunning(val) {
      if (!val) {
        this.$notifyInfo('It was not possible to retrieve keyspaces <br> - make sure Grakn is running <br> - check that host and port in connection settings are correct');
      }
    },
    showKeyspaceList(show) {
      // Close keyspaces list when user clicks anywhere else
      if (show) window.addEventListener('click', this.clickEvent);
      else window.removeEventListener('click', this.clickEvent);
    },
  },
  methods: {
    setKeyspace(name) {
      this.$emit('keyspace-selected');
      storage.set('current_keyspace_data', name);
      this[CURRENT_KEYSPACE_CHANGED](name);
      this.showKeyspaceList = false;
    },
    toggleKeyspaceList() {
      this.$emit('keyspace-selected');
      this.showKeyspaceList = !this.showKeyspaceList;
    },
  },
};
</script>
