<template>
    <div class="wrapper">
      <img @click="togglePanel" src="static/img/icons/icon_add_white.svg" class="btn new-keyspace-button" id="create-keyspace-btn">
      <transition name="slide-fade">
          <div v-if="showCreateKeyspacePanel" class= "new-keyspace-panel">
          <div class="field flex-1">
            <input id="keyspace-name" class="grakn-input flex-1" placeholder="Keyspace Name" v-model="newKeyspaceName" maxlength="48">
          </div>
          <div class="button-line">
            <loading-button id="create-btn" :clickFunction="newKeyspace" value="Create" :isLoading="isLoading" :disabled="!newKeyspaceName.length"></loading-button>
          </div>
        </div>
      </transition>
    </div>
</template>
<style scoped>

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

.field {
    padding: 5px;
    margin-bottom: 10px;
    display: flex;
    flex-direction: column;
}

.small{
  font-size: 80%;
  padding: 2px;
}

.wrapper{
  padding: 5px;
  display: flex;
  position: relative;
  margin-top: 2.5%;
}

.button-line{
  display: flex;
  justify-content: flex-end;
}

.new-keyspace-button{
  cursor: pointer;
  height: 30px;
  width: 30px;
  padding: 0;
}

.new-keyspace-panel{
  background: #282828;
  position: absolute;
  padding: 5px;
  top: 100%;
  width: 90%;
}

</style>

<script>
export default {
  name: 'CreateKeyspaceCard',
  data() {
    return {
      isLoading: false,
      newKeyspaceName: '',
      showCreateKeyspacePanel: false,
    };
  },
  methods: {
    newKeyspace() {
      if (!this.newKeyspaceName.length) return;
      this.isLoading = true;
      const ksName = this.newKeyspaceName;
      this.newKeyspaceName = '';
      this.$store.dispatch('createKeyspace', ksName)
        .then(() => { this.$notifySuccess(`New keyspace [${ksName}] successfully created!`); })
        .catch((error) => { this.$notifyError(error, 'Create keyspace'); })
        .then(() => { this.isLoading = false; });
    },
    togglePanel() {
      this.showCreateKeyspacePanel = !this.showCreateKeyspacePanel;
    },
  },
};
</script>
