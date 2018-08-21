<template>
  <div class="bottom-bar-wrapper">
    <div class="bottom-bar-nav">
      <div class="bottom-bar-caret" @click="toggleBottomBar"><caret-icon  :toggleNorth="minimizeBottomBar"></caret-icon></div>
    </div>
    <div v-if="!minimizeBottomBar" class="content">
    </div>
  </div>
</template>

<style scoped>
.bottom-bar-wrapper {
  width: 100%;
  /* height: 150px; */
  background-color: #1A1A1A;
  position: relative;
  border-top: 1px solid white;
}

.bottom-bar-nav{
  width: 100%;
  height: 20px;
  border-bottom: 1px solid white;
  display: flex;
  flex-direction: row;
}

.bottom-bar-caret {
  width: 20px;
  position: relative;
  left: 98%;
}

.content {
  height: 150px;
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
