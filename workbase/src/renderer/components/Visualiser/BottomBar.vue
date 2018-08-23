<template>
  <div class="bottom-bar-wrapper">
    <div class="bottom-bar-nav">
      <img class="bottom-bar-arrow" @click="toggleBottomBar" :src="(minimizeBottomBar) ? 'static/img/icons/icon_up_arrow.svg' : 'static/img/icons/icon_down_arrow.svg'">
    </div>
    <div v-if="!minimizeBottomBar" class="content">
    </div>
  </div>
</template>

<style lang="scss" scoped>
$default-width: 150px;
$nav-height: 20px;

.bottom-bar-wrapper {
  width: 100%;
  background-color: var(--light-color);
  position: relative;
  border-top: var(--container-border);
}

.bottom-bar-nav{
  width: 100%;
  height: $nav-height;
  border-bottom: var(--container-border);
  flex-direction: row;
}

.bottom-bar-arrow {
  height: 20px;
  position: relative;
  cursor: pointer;
  float: right;
}

.content {
  height: $default-width;
}

</style>

<script>
import CaretIcon from '@/components/UIElements/CaretIcon.vue';


export default {
  components: { CaretIcon },
  data() {
    return {
      minimizeBottomBar: false,
    };
  },
  methods: {
    toggleBottomBar() {
      this.minimizeBottomBar = !this.minimizeBottomBar;
    },
  },
};
</script>
