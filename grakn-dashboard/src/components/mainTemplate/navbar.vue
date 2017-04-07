<template>
<div>
    <nav role="navigation" class="navbar-fixed z-depth-1">
        <div class="nav-wrapper">
            <div class="list-logo">
              <img @click="toggleSideBar" src="/img/logo-text.png" class="logo-img"></img>
            </div>
            <!-- <div class="dynamic-center-left">
                <keep-alive>
                    <component :is="componentCenterLeft"></component>
                </keep-alive>
            </div> -->
            <div class="dynamic-center">
                <keep-alive>
                    <component :is="componentCenter"></component>
                </keep-alive>
            </div>
            <div class="dynamic-center-right">
                <component :is="componentCenterRight"></component>
            </div>
        </div>
    </nav>
</div>
</template>

<style>
.logo-img {
    height: 38px;
    width: 174px;
    border-radius: 10px;
    display: inline-flex;
    flex-shrink: 1;
    margin: 5px 0px;
}

.nav-wrapper {
    display: flex;
    flex-wrap: nowrap;
    justify-content: space-between;
    list-style: none;
    padding: 0px 10px;
}

.list-logo {
    font-weight: bold;
    text-decoration: none;
    display: inline-flex;
    justify-content: flex-start;
    align-items: center;
    flex: 1;
}

.dynamic-center {
    display: inline-flex;
    flex: 3;
}

.dynamic-center-left {
    display: inline-flex;
    flex: 1;
}

.dynamic-center-right {
    display: inline-flex;
    flex: 1;
    align-items: center;
    justify-content: flex-end;
}


/*Navbar fixed properties*/

.navbar-fixed {
    position: relative;
    z-index: 997;
    background-color: #0f0f0f;
}

nav a {
    color: #fff;
}


/*------------*/

.c-hamburger {
    position: relative;
    overflow: hidden;
    width: 48px;
    height: 48px;
    font-size: 0;
    border-radius: 2px;
    border: none;
    cursor: pointer;
    transition: background 0.3s;
}

.c-hamburger:focus {
    outline: none;
}

.c-hamburger span {
    display: block;
    position: absolute;
    top: 22px;
    left: 9px;
    right: 9px;
    height: 4px;
    background: white;
}

.c-hamburger span::before,
.c-hamburger span::after {
    position: absolute;
    display: block;
    left: 0;
    width: 100%;
    height: 4px;
    background-color: #fff;
    content: "";
}

.c-hamburger span::before {
    top: -10px;
}

.c-hamburger span::after {
    bottom: -10px;
}
</style>

<script>
import User from '../../js/User.js';
const GraqlEditor = require('../graqlEditor/graqlEditor.vue');
const KeyspacesSelect = require('../keyspacesSelect.vue');


export default {
  props: ['componentCenterLeft', 'componentCenterRight', 'componentCenter'],
  components: {
    GraqlEditor,
    KeyspacesSelect,
  },
  data() {
    return {
      isUserAuth: User.isAuthenticated(),
      hamburgerActive: false,
    };
  },
  created() {},
  mounted() {
    this.$nextTick(() => {
    });
  },
  methods: {
    logout() {
      User.logout();
      this.$router.push('/login');
    },
    toggleSideBar() {
      this.$emit('toogle-sidebar');
      this.hamburgerActive = !this.hamburgerActive;
    },
  },
};
</script>
