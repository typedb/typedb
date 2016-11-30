/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

// change this to alias Vue instead of import vue/dist - check out Aliasify
import Vue from 'vue/dist/vue.js'
var VueRouter = require('vue-router')

// Vue.config.devtools = false;
Vue.config.errorHandler = function(err, vm) {
    console.log("Something is wrong here!!!! " + JSON.stringify(err));
}
Vue.use(VueRouter)


var bus = new Vue();


// Components
const graknapp = require('./components/main.vue');
const visualiser = require('./components/visualiser.vue')
const console = require('./components/console.vue')
const config = require('./components/config.vue')

const routes = [{
    path: '/',
    redirect: '/graph'
}, {
    path: '/config',
    component: config
}, {
    path: '/graph',
    component: visualiser
}
, {
    path: '/console',
    component: console
}
]

var router = new VueRouter({
    linkActiveClass: 'active',
    routes
})

new Vue({
    el: '#grakn-app',
    router: router,
    render: h => h(graknapp)
})
