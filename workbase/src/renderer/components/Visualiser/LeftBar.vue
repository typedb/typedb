<template>
  <div class="left-bar-container">
    <div class="minimize-command-bar" @click="toggleCommandsBar">
      <img class="minimize-arrow" :src="(minimizeCommandsBar) ? 'static/img/icons/icon_right_arrow.png' : 'static/img/icons/icon_left_arrow.png'">
    </div>
    <div v-if="!minimizeCommandsBar" class="title">
      commands
    </div>
    <div class="command-list">
      <div class="command-item">
        <!-- <button class="btn command-btn">M</button>
        <div v-if="!minimizeCommandsBar" class="command-label">Match</div> -->
      </div>

    </div>
  </div>
</template>

<style scoped>

.left-bar-container {
  height: 100%;
  background-color: var(--light-color);
  position: relative;
  padding: var(--container-padding);
  border-right: var(--container-border);
}

.minimize-command-bar {
  width: 18px;
  height: 22px;
  position: absolute;
  background-color: var(--light-color);
  left: 100%;
  display: flex;
  align-items: center;
  justify-content: center;
  border-right: var(--container-border);
  border-top: var(--container-border);
  border-bottom: var(--container-border);
  cursor: pointer;
  z-index:1;
}

.minimize-arrow{
  height: 20px;
}

.command-list {
  display: flex;
  flex-direction: column;
}

.command-item {
  display: flex;
  flex-direction: row;
}

.command-btn {
  height: 30px;
  width: 30px;
}

.command-label {
  border-bottom: 1px solid white;
  margin-top: 22px;
  white-space: nowrap;
}

.title {
  text-align: center;
  margin-top: 10px;
  margin-bottom: 10px;
}

</style>

<script>
import CaretIcon from '@/components/UIElements/CaretIcon.vue';


export default {
  components: { CaretIcon },
  data() {
    return {
      minimizeCommandsBar: false,
    };
  },
  methods: {
    toggleCommandsBar() {
      this.minimizeCommandsBar = !this.minimizeCommandsBar;
    },
  },
};
</script>
