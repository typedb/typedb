/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

import VeeValidate from 'vee-validate';
// change this to alias Vue instead of import vue/dist - check out Aliasify
import Vue from 'vue/dist/vue';
import User from './js/User';
import EngineClient from './js/EngineClient';

const VueRouter = require('vue-router');

Vue.use(VueRouter);
Vue.use(VeeValidate);

// Components
const graphPage = require('./components/graphPage.vue');
const consolePage = require('./components/consolePage.vue');
const configPage = require('./components/configPage.vue');
const loginPage = require('./components/loginPage.vue');
const sidebar = require('./components/global/sidebar.vue');
const keyspacesmodal = require('./components/global/keyspacesModal.vue');
const signupmodal = require('./components/global/signupModal.vue');

// ---------------------- Vue setup ---------------------//

const routes = [{
  path: '/',
  redirect: '/graph',
}, {
  path: '/config',
  component: configPage,
}, {
  path: '/graph',
  component: graphPage,
}, {
  path: '/console',
  component: consolePage,
}, {
  path: '/login',
  component: loginPage,
}];

const router = new VueRouter({
  linkActiveClass: 'active',
  routes,
});

let authNeeded;

// Before loading every page we need to check if Authentication is enabled, if yes the user must be logged in.
router.beforeEach((to, from, next) => {
  if (authNeeded === undefined) {
    EngineClient.request({
      url: '/auth/enabled/',
      callback: (resp, error) => {
        authNeeded = resp;
        if (authNeeded) {
          next('/login');
        } else {
          next();
        }
      },
    });
  } else if (User.isAuthenticated() || authNeeded === false || to.path === '/login') {
    next();
  } else {
    next('/login');
  }
});

Vue.component('side-bar', {
  render: h => h(sidebar),
});

Vue.component('keyspaces-modal', {
  render: h => h(keyspacesmodal),
});

Vue.component('signup-modal', {
  render: h => h(signupmodal),
});

new Vue({
  router,
}).$mount('#grakn-app');
