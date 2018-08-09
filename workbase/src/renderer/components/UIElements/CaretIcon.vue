<template>
    <div class="icon icon--caret" :class="{'toggle--north':toggleNorth}">
        <span class="line--vertical"></span>
        <span class="line--horizontal"></span>
    </div>
</template>
<script>

export default {
  name: 'CaretIcon',
  props: ['toggleNorth'],
};
</script>
<style lang="scss" scoped>

.icon {
  display: inline-block;
  position: relative;
  cursor: pointer;
  padding: 12px;
  margin-left: auto;
  transition: 100ms;

  &.icon--caret {
   
    &::before {
      position: absolute;
      content: '';
      width: 1px;
      height: 10px;
      background: white;
      display: inline-block;
      transition: 300ms;
      top: 30%;
      left: 32%;
      transform: rotateZ(45deg);
    }

    &::after {
      position: absolute;
      content: '';
      width: 1px;
      height: 10px;
      background: white;
      display: inline-block;
      transition: 300ms;
      top: 30%;
      left: 3%;
      transform: rotateZ(-45deg);
    }
    
    &.toggle--north {
      &::before {
        transform: rotateZ(-45deg);
      }
      &::after {
        transform: rotateZ(45deg);
      }
    }
  }
}
</style>
