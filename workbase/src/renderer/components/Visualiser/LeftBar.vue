<template>
  <div class="left-bar-container">
    <div class="minimize-command-bar" @click="toggleCommandsBar">
      <!--<img class="minimize-arrow" :src="(minimizeCommandsBar) ? 'static/img/icons/icon_right_arrow.png' : 'static/img/icons/icon_left_arrow.png'">-->
    </div>
    <div class="command-list">
      <div class="command-item">
      </div>
    </div>
  </div>
</template>

<style scoped>

.left-bar-container {
  background-color: var(--gray-1);
  padding: var(--container-padding);
  border-right: var(--container-darkest-border);
  height: 100%;
  position: relative;
}

.minimize-command-bar {
  background-color: var(--gray-1);
  border-right: var(--container-darkest-border);
  border-top: var(--container-darkest-border);
  border-bottom: var(--container-darkest-border);
  width: 18px;
  height: 22px;
  position: absolute;
  left: 100%;
  display: flex;
  align-items: center;
  justify-content: center;
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

</style>

<script>

export default {
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
