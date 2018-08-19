<template>
    <div class="wrapper" v-show="showPanel">
        <div v-for="concept in instances" :key="concept" class="line">
            <span>{{concept}}</span>
            <span class="btn" @click="removeSchemaConcept(concept)"><i class="fas fa-trash-alt"></i></span>
        </div>
    </div>
</template>
<style scoped>
.line{
    display: flex;
    flex-direction: row;
    align-items: center;
    justify-content: space-between;
}
.wrapper{
  position: absolute;
  z-index: 5;
  left: 5px;
  top: 5px;
  background-color: #282828;
  max-height: 100%;
  width: 220px;
  overflow: scroll;
  border-radius: 2px;
  padding: 10px 10px;
}
.fa-trash-alt {
    font-size: 95%;
}
</style>
<script>
export default {
  props: ['instances', 'showPanel', 'concept'],
  methods: {
    removeSchemaConcept(label) {
      this.$notifyConfirmDelete(`This operation will delete the ${this.concept} [${label}] from all the existing types. <br> Are you sure you want to proceed?`,
        () => this.$emit('delete-schema-concept', { label }));
    },
  },
};
</script>
