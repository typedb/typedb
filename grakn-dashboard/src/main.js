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

// change this to alias Vue instead of import vue/dist - check out Aliasify
import Vue from 'vue/dist/vue.js'
import User from './js/User.js'
import EngineClient from './js/EngineClient.js';


var VueRouter = require('vue-router')

// Vue.config.devtools = false;

Vue.use(VueRouter)


// Components
const graknapp = require('./components/main.vue');
const visualiser = require('./components/visualiser.vue')
const console = require('./components/console.vue')
const config = require('./components/config.vue')
const login = require('./components/login.vue')
const sidebar = require('./components/sidebar.vue')
const keyspacesmodal = require('./components/keyspacesModal.vue')


const routes = [{
    path: '/',
    redirect: '/graph'
}, {
    path: '/config',
    component: config
}, {
    path: '/graph',
    component: visualiser
}, {
    path: '/console',
    component: console
}, {
    path: '/login',
    component: login
}]

const router = new VueRouter({
    linkActiveClass: 'active',
    routes
})

var authNeeded = undefined;

router.beforeEach((to, from, next) => {
    if (authNeeded === undefined && !User.isAuthenticated()) {
        let engineClient = new EngineClient();
        engineClient.request({
            url: "/auth/enabled/",
            callback: (resp, error) => {
                authNeeded =resp;
                if (authNeeded) {
                    next('/login');
                } else {
                    next();
                }
            }
        });
    } else {
        if ( User.isAuthenticated() || authNeeded == false || to.path === "/login") {
            next();
        } else {
            next('/login');
        }
    }
})

Vue.component('side-bar', {
    render: h => h(sidebar)
})

Vue.component('keyspaces-modal', {
    render: h => h(keyspacesmodal)
})

const graknDashboard = new Vue({
    router: router
}).$mount('#grakn-app')
