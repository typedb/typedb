<template>
<div class="side-wrapper">
  <transition name="slide-left" appear>
    <aside>
        <nav class="side-vert-flex">
            <ul class="nav">
                <li>
                    <panel-container title="INFO"><info :localStore="localStore"></info></panel-container>
                </li>
                <li>
                    <panel-container title="ATTRIBUTES"><attributes :localStore="localStore"></attributes></panel-container>
                </li>
                <li>
                    <panel-container title="PLAYS"><plays :localStore="localStore"></plays></panel-container>
                </li>
                <li>
                    <panel-container title="RELATES" :disablePanel="!showRelates"><relates :localStore="localStore"></relates></panel-container>
                </li>
            </ul>
        </nav>
    </aside>
  </transition>
</div>
</template>

<style scoped>
aside {
    width: 250px;
}
.side-wrapper{
    position: absolute;
    top: 0px;
    right: 0px;
}

.side-vert-flex {
    display: flex;
    flex-direction: column;
}

ul.nav {
    display: flex;
    flex-direction: column;
}

</style>

<script>
import PanelContainer from '@/components/shared/PanelContainer.vue';

import Info from './NodePanel/Info.vue';
import Attributes from './NodePanel/Attributes.vue';
import Plays from './NodePanel/Plays.vue';
import Relates from './NodePanel/Relates.vue';

export default {
  components: {
    Info, Attributes, PanelContainer, Plays, Relates,
  },
  props: ['localStore'],
  data() {
    return {
    };
  },
  created() {},
  computed: {
    showRelates() {
      const node = this.localStore.getSelectedNode();
      return (node && node.baseType === 'RELATIONSHIP_TYPE');
    },
  },
  mounted() {
    this.$nextTick(() => {
    });
  },
  methods: {
  },
};
</script>
