<template>
  <div class="list">
    <div class="line">
      <div class="label">Host</div> 
      <div><input class="grakn-input text-right" v-model="engineHost"></div>
    </div>
    <div class="line">
      <div class="label">Port</div>
      <div><input class="grakn-input text-right" v-model="engineGrpcPort"></div>
    </div>
  </div>
</template>

<style scoped>

.list {
    display: flex;
    flex-direction: column;
    padding: 10px 5px;
 }
 .line {
    padding: 4px;
    display: flex;
    justify-content: space-around;
    align-items: center;
}
.label{
  margin: 0 5px;
}

.text-right{
  text-align: right;
  width: 140px;
}

</style>

<script>
  import Settings from '../../EngineSettings';
  
  export default {
    name: 'EngineTab',
    data() {
      return {
        engineHost: undefined,
        engineGrpcPort: undefined,
      };
    },
  
    created() {
    },
  
    mounted() {
      this.$nextTick(() => {
        this.engineHost = Settings.getEngineHost();
        this.engineGrpcPort = Settings.getEngineGrpcPort();
      });
    },
    watch: {
      engineHost(newVal) {
        Settings.setEngineHost(newVal);
      },
      engineGrpcPort(newVal) {
        Settings.setEngineGrpcPort(newVal);
      },
    },
  };
</script>
