<template>
  <div class="right-bar-wrapper">

  </div>
</template>

<style lang="scss" scoped>
$bar-max-width: 150px;
$bar-min-width: 150px;

.right-bar-wrapper {
  max-width: $bar-max-width;
  min-width: $bar-min-width;
  height: 100%;
  background-color: var(--light-color);
  position: relative;
  border-left: var(--container-border);
  resize: horizontal;
      overflow: auto;
}

</style>

<script>


export default {

};
</script>
