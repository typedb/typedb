<template>
    <transition name="fade" appear>
        <div>
            <div class="vis-tabs">
              <button @click="newTab" class="btn">New Tab</button>
            </div>
            <vis-tab v-if="test" :tabId="0"></vis-tab>
            <vis-tab v-else :tabId="1"></vis-tab>
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
      test: true,
    };
  },
  methods: {
    newTab() {
      this.test = !this.test;
    },
  },
};
</script>
