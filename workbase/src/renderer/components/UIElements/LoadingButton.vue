<template>
    <div class="loading-button btn btn-success noselect" @click="clickFunction">
      <i class="btn-spinner" v-show="isLoading"></i>
      <span class="btn-label" v-show="!isLoading">{{value}}</span>
    </div>
</template>
<script>

export default {
  name: 'LoadingButton',
  props: ['value', 'isLoading', 'clickFunction'],
};
</script>


<style lang="scss" scoped>

.loading-button{
  cursor: pointer;
}

// size of small spinner
$spinner-size: 15px;

// border size of small spinner
$spinner-border-size: 2px;

// spinner colors
$spinner-inverse-color: #FFF;
$spinner-default-color: #333;
$spinner-accent-color: #4873cc; // blue

@-webkit-keyframes rotation {
   from { -webkit-transform: rotate(0deg); }
   to { -webkit-transform: rotate(359deg); }
}

@keyframes rotation {
   from { transform: rotate(0deg); }
   to { transform: rotate(359deg); }
}

.btn-spinner {
  // Make sure we use content box model for this
  &, &:before, &:after {
    -moz-box-sizing: border-box;
    -webkit-box-sizing: border-box;
    box-sizing: content-box;
  }
  height: $spinner-size;
  width: $spinner-size;
  display: inline-block;
  position: relative;
  top: 2px;
  -webkit-animation: rotation 1s infinite linear;
  animation: rotation .7s infinite linear;
  border: $spinner-border-size solid lighten($spinner-accent-color,25%);
  border-radius: 100%;
  &:before {
     content:"";
     display:block;
     position:absolute;
     left: -$spinner-border-size;
     top: -$spinner-border-size;
     height: 100%;
     width: 100%;
     border-top: $spinner-border-size solid $spinner-accent-color;
     border-left: $spinner-border-size solid transparent;
     border-bottom: $spinner-border-size solid transparent;
     border-right: $spinner-border-size solid transparent;
     border-radius: 100%;
  }
}

.btn-default .spinner {
  border-color: rgba(0,0,0,0.33);
  &:before {
    border-top-color: $spinner-default-color;
  }
}

.btn-spinner {
  border-color: rgba($spinner-inverse-color,0.33);
  &:before {
    border-top-color: $spinner-inverse-color;
  }
}
</style>
