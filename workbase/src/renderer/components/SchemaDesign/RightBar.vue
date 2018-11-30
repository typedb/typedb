<template>
  <div class="right-bar-container">
    <div class="minimize-right-bar" v-bind:style= "[showRightBar ? {} : {'opacity': '1'}]" @click="showRightBar = !showRightBar">
      <vue-icon :icon="(showRightBar) ? 'double-chevron-right' : 'double-chevron-left'" iconSize="14" className="vue-icon"></vue-icon>
    </div>
    <div class="content" v-if="showRightBar">
      <identity-panel></identity-panel>
      <attributes-panel></attributes-panel>
    </div>
  </div>
</template>

<style scoped lang="scss">

  .content {
    width: 201px;
  }

  .right-bar-container {
    background-color: var(--gray-3);
    border-left: var(--container-darkest-border);
    height: 100%;
    position: relative;
    right: 0px;
    top: 0px;
    z-index: 1;
  }

  .minimize-right-bar {
    background-color: var(--gray-1);
    border-right: var(--container-darkest-border);
    border-top: var(--container-darkest-border);
    border-bottom: var(--container-darkest-border);
    width: 18px;
    height: 30px;
    position: absolute;
    right: 100%;
    top: 50%;
    opacity: 0;
    display: flex;
    align-items: center;
    justify-content: center;
    cursor: pointer;
    z-index:1;
}


.right-bar-container:hover {
  .minimize-right-bar {
    opacity: 1;
  }
}

</style>

<script>
  import IdentityPanel from './RightBar/IdentityPanel';
  import AttributesPanel from './RightBar/AttributesPanel';

  export default {
    components: { IdentityPanel, AttributesPanel },
    data() {
      return {
        showRightBar: true,
      };
    },
  };
</script>
