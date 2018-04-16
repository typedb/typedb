/*-
 * #%L
 * grakn-dashboard
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */
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
import User, { DEFAULT_KEYSPACE } from './js/User';
import EngineClient from './js/EngineClient';
import routes from './routes';

Array.prototype.flatMap = function (lambda) {
  return Array.prototype.concat.apply([], this.map(lambda));
};


Vue.use(VueRouter);
Vue.use(VeeValidate);

// Define a Vue Router and map all the routes to components - as defined in the routes.js file.
const router = new VueRouter({
  linkActiveClass: 'active',
  routes,
});


// Check if the currentKeyspace is in the list of keyspaces sent from grakn
// If not, set the currentKeyspace to the default one.
const checkCurrentKeySpace = () => EngineClient.fetchKeyspaces().then((resp) => {
  const keyspaces = JSON.parse(resp).keyspaces.map(ks => ks.name);
  if (!keyspaces.includes(User.getCurrentKeySpace())) {
    User.setCurrentKeySpace(DEFAULT_KEYSPACE);
  }
});

// Middleware to ensure:
// - current keyspace saved in localStorage is still available in Grakn
router.beforeEach((to, from, next) => {
  checkCurrentKeySpace()
    .then(() => { next(); });
});

new Vue({
  router,
}).$mount('#grakn-app');
