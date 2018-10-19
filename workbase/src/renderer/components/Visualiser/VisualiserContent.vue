<template>
    <transition name="fade" appear>
        <div>
            <vis-tab :tabId="currentTab"></vis-tab>
        </div>
    </transition>
</template>

<style scoped>
</style>

<script>

import VisTab from './VisTab.vue';

export default {
  name: 'VisualiserContent',
  components: { VisTab },
  data() {
    return {
      currentTab: 0,
    };
  },
  computed: {
  },
};
</script>
