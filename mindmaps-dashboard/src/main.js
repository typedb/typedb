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

var Vue = require('vue')
var VueRouter = require('vue-router')

// Components
var app = require('./components/main.vue');
var visualiser = require('./components/visualiser.vue')
var status =  require('./components/status.vue')

Vue.use(VueRouter)

var router = new VueRouter({
    hashbang: false,
    linkActiveClass: 'active'
})
router.map({
    '/status': {
        component: status
    },
    '/shell': {
        component: visualiser
    }
})
router.redirect({
    // home page
    '*': '/status'
})

router.start(app, '#app')
