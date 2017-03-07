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

import Vue from 'vue';
import VeeValidate from 'vee-validate';
import VueRouter from 'vue-router';

// Modules
import User from './js/User';
import EngineClient from './js/EngineClient';
import routes from './routes';


Vue.use(VueRouter);
Vue.use(VeeValidate);

// Define a Vue Router and map all the routes to components - as defined in the routes.js file.
const router = new VueRouter({
  linkActiveClass: 'active',
  routes,
});

let authNeeded;

// Functino used to ask Engine if a token is needed to use its APIs
const checkIfAuthNeeded = function contactEngine(next) {
  EngineClient.request({
    url: '/auth/enabled/',
  }).then((result) => {
    authNeeded = (result === 'true');
    if (authNeeded === false) {
      next();
    } else {
      next('/login');
    }
  }, () => {});
};

// Middleware to ensure the user is authenticated when needed.
router.beforeEach((to, from, next) => {
  if (authNeeded === undefined) {
    checkIfAuthNeeded(next);
  } else if (User.isAuthenticated() || authNeeded === false || to.path === '/login') {
    next();
  } else {
    next('/login');
  }
});

new Vue({
  router,
}).$mount('#grakn-app');
