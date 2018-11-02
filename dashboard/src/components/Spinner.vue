<template>
<div id="loading-spinner">
  <svg class="spinner" viewBox="0 0 50 50">
  <circle class="path" cx="25" cy="25" r="20" fill="none" stroke-width="5"></circle>
  </svg>
</div>
</template>

<style scoped>
#loading-spinner{
  visibility: hidden;
  z-index: 2;
  position: absolute;
  top: 115%;
  left: -30px;
}

.spinner {
  -webkit-animation: rotate 1.5s linear infinite;
          animation: rotate 1.5s linear infinite;
  width: 30px;
  height: 30px;
}
.spinner .path {
  stroke: #00eca2;
  stroke-linecap: round;
  -webkit-animation: dash 1.0s ease-in-out infinite;
          animation: dash 1.0s ease-in-out infinite;
}

@-webkit-keyframes rotate {
  100% {
    -webkit-transform: rotate(360deg);
            transform: rotate(360deg);
  }
}

@keyframes rotate {
  100% {
    -webkit-transform: rotate(360deg);
            transform: rotate(360deg);
  }
}
@-webkit-keyframes dash {
  0% {
    stroke-dasharray: 1, 150;
    stroke-dashoffset: 0;
  }
  50% {
    stroke-dasharray: 90, 150;
    stroke-dashoffset: -35;
  }
  100% {
    stroke-dasharray: 90, 150;
    stroke-dashoffset: -124;
  }
}
@keyframes dash {
  0% {
    stroke-dasharray: 1, 150;
    stroke-dashoffset: 0;
  }
  50% {
    stroke-dasharray: 90, 150;
    stroke-dashoffset: -35;
  }
  100% {
    stroke-dasharray: 90, 150;
    stroke-dashoffset: -124;
  }
}

</style>
