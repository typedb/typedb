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


// Import pages components
const mainTemplate = require('./components/mainTemplate/mainTemplate.vue');
const graphPage = require('./components/graphPage/graphPage.vue');
const consolePage = require('./components/consolePage.vue');
const tasksPage = require('./components/tasksPage/tasksPage.vue');
const configPage = require('./components/configPage.vue');
const loginPage = require('./components/loginPage.vue');
const notFoundPage = require('./components/notFoundPage.vue');

// Routes
const routes = [{
  path: '/login',
  component: loginPage,
  login: true,
},
{
  path: '/',
  component: mainTemplate,
  children: [{
    path: '/graph',
    component: graphPage,
    name: 'Graph',
    description: 'Graph visualiser page',
  }, {
    path: '/config',
    component: configPage,
    name: 'Config',
    description: 'Engine configurations page',
  }, {
    path: '/console',
    component: consolePage,
    name: 'Console',
    description: 'Graql console page',
  },
  {
    path: '/tasks',
    component: tasksPage,
    name: 'Tasks',
    description: 'Page to manage Engine tasks',
  },
  {
    path: '',
    redirect: '/graph',
    description: 'Redirect to the first page. For now the graph page.',
  }],
},
{
  path: '*',
  component: notFoundPage,
  description: 'Catch all the routes that are not mapped and show 404 page.',
}];

export default routes;
