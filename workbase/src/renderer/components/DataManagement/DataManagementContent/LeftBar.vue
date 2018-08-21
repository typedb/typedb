<template>
  <div class="left-bar-wrapper">
    <div class="minimize-commands-bar" @click="toggleCommandsBar">
      <caret-icon class="commands-caret" :toggleNorth="minimizeCommandsBar"></caret-icon>
    </div>
    <div v-if="!minimizeCommandsBar" class="title">
      commands
    </div>
    <div class="commands-list">
      <div class="command-item">
        <button class="btn command-btn">M</button>
        <div v-if="!minimizeCommandsBar" class="command-label">Match</div>
      </div>
      <div class="command-item">
        <button class="btn command-btn">I</button>
        <div v-if="!minimizeCommandsBar" class="command-label">Insert</div>
      </div>
      <div class="command-item">
        <button class="btn command-btn">E</button>
        <div v-if="!minimizeCommandsBar" class="command-label">Explain</div>
      </div>
      <div class="command-item">
        <button class="btn command-btn">S</button>
        <div v-if="!minimizeCommandsBar" class="command-label">Shortest Path</div>
      </div>
      <div class="command-item">
        <button class="btn command-btn">X</button>
        <div v-if="!minimizeCommandsBar" class="command-label">Min</div>
      </div>
      <div class="command-item">
        <button class="btn command-btn">H</button>
        <div v-if="!minimizeCommandsBar" class="command-label">Max</div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.left-bar-wrapper {
  height: 100%;
  background-color: #1A1A1A;
  position: relative;
  border-right: 1px solid white;
  padding: 5px;
}

.minimize-commands-bar {
  width: 13px;
  height: 22px;
  position: absolute;
  left:100%;
  background-color: #1A1A1A;
}

.commands-caret {
  -webkit-transform: rotate(90deg);
  -moz-transform: rotate(90deg);
  -o-transform: rotate(90deg);
  -ms-transform: rotate(90deg);
  transform: rotate(90deg);
  margin-top: 6px;
  right: 6px;
}

.commands-list {
  display: flex;
  flex-direction: column;
}

.command-item {
  display: flex;
  flex-direction: row;
}

.command-btn {
  margin-right: 5px;
  height: 30px;
}

.command-label {
  border-bottom: 1px solid white;
  margin-top: 15px;
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
