<!--
Grakn - A Distributed Semantic Database
Copyright (C) 2016  Grakn Labs Limited

Grakn is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

Grakn is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
-->


<template>
<div class="wrapper">
  <signup-modal></signup-modal>

    <div class="navigation">
        <nav-bar :componentCenterLeft="componentCenterLeft" :componentCenter="componentCenter" :componentCenterRight="componentCenterRight" v-on:toogle-sidebar="showSideBar=!showSideBar"></nav-bar>
    </div>
    <div class="content">
        <side-bar :showSideBar="showSideBar"></side-bar>
        <keep-alive include="GraphPage">
            <router-view></router-view>
        </keep-alive>
    </div>
</div>
</template>

<style>
.wrapper {
    display: flex;
    flex-direction: column;
    height: 100vh;
}

.content {
    display: flex;
    flex-direction: column;
    flex: 1;
    height: 100%;
    position: relative;
}
</style>

<script>
// Sub-components
const NavBar = require('./navbar.vue');
const SignupModal = require('./signupModal.vue');
const SideBar = require('./sidebar.vue');


const computeComponentCenter = function(path) {
    switch (path) {
        case '/graph':
        case '/console':
            return 'GraqlEditor';
            break;
        default:
            return null;
    }
};
const computeComponentCenterRight = function(path) {
  switch (path) {
      case '/graph':
      case '/console':
          return 'KeyspacesSelect';
          break;
      default:
          return null;
  }
};

// TODO: decide whether keep this component or not. Not used for now.
const computeComponentCenterLeft = function(path) {
    return null;
};

import User from '../../js/User';


export default {
    name: 'MainTemplate',
    components: {
        NavBar,
        SignupModal,
        SideBar,
    },
    beforeRouteEnter(to, from, next) {
        next(vm => {
            vm.componentCenterLeft = computeComponentCenterLeft(to.path);
            vm.componentCenterRight = computeComponentCenterRight(to.path);
            vm.componentCenter = computeComponentCenter(to.path);
        })
    },
    data() {
        return {
            componentCenterLeft: undefined,
            componentCenterRight: undefined,
            componentCenter: undefined,
            showSideBar: false,
        };
    },

    created() {},

    mounted() {
        this.$nextTick(function nextTickVisualiser() {
          // Initialise toastr.js plugin to use custom options
          toastr.options = {
              "closeButton": true,
              "debug": false,
              "newestOnTop": false,
              "progressBar": false,
              "positionClass": "toast-bottom-right",
              "preventDuplicates": false,
              "showDuration": "300",
              "hideDuration": "1000",
              "timeOut": "3000",
              "extendedTimeOut": "1000",
              "showEasing": "swing",
              "hideEasing": "linear",
              "showMethod": "fadeIn",
              "hideMethod": "fadeOut"
          };

          if (!User.getModalShown()){
            var modal = document.getElementById('myModal');
            modal.style.display = "block";
            User.setModalShown(true);
          }

        });
    },
    watch: {
        // When the route changes we need to tell the navbar what to inject in computeComponentCenter && componentCenterLeft && componentCenterRight
        // based on the component we are rendering in the router-view
        '$route': function(newRoute) {
            this.componentCenterLeft = computeComponentCenterLeft(newRoute.path);
            this.componentCenterRight = computeComponentCenterRight(newRoute.path);
            this.componentCenter = computeComponentCenter(newRoute.path);
        }
    },

    methods: {

    },
};
</script>
