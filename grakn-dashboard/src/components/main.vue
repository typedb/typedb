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
<section class="wrapper">

    <!-- Header-->
    <!--<nav class="navbar navbar-default navbar-fixed-top">-->
        <!--<div class="container-fluid">-->
            <!--<div class="navbar-header">-->
                <!--<div id="mobile-menu">-->
                    <!--<div class="left-nav-toggle">-->
                        <!--<a href="#">-->
                            <!--<i class="stroke-hamburgermenu"></i>-->
                        <!--</a>-->
                    <!--</div>-->
                <!--</div>-->
                <!--<a class="navbar-brand" href="/">-->
                    <!--Grakn-->
                    <!--<span>{{version}}</span>-->
                <!--</a>-->
            <!--</div>-->
            <!--<div id="navbar" class="navbar-collapse collapse">-->
                <!--<div class="left-nav-toggle">-->
                    <!--<a href="">-->
                        <!--<i class="stroke-hamburgermenu"></i>-->
                    <!--</a>-->
                <!--</div>-->
            <!--</div>-->
        <!--</div>-->
    <!--</nav>-->
    <!-- End header-->

    <!-- Navigation-->
    <aside class="navigation">
        <div class="brand" href="/">
            GRAKN.AI
            <span>{{version}}</span>
        </div>
        <nav>
            <ul class="nav luna-nav">
                <li v-link-active>
                    <a v-link="{ path: '/graph' }">Graph</a>
                </li>

                <li v-link-active>
                    <a v-link="{ path: '/console' }">Console</a>
                </li>

                <li v-link-active>
                    <a v-link="{ path: '/config' }">Config</a>
                </li>

                <li v-link-active>
                    <a target="_blank" href="https://grakn.ai/pages/index.html">Documentation</a>
                </li>

                <li class="nav-info">
                    <div class="m-t-xs">
                        <!-- <span class="c-white">Example</span> text. -->
                        <br/>
                        <!-- HOW TO REMOVE THIS SECTION -->
                        <!--<span>
                            If you don't want to have this section, just remove it from html and in your css replace:
                            .navigation:before { background-color: #24262d; } with
                            .navigation:before { background-color: #2a2d35; }
                            and
                            .navigation { background-color: #24262d; }</code> with <code>.navigation { background-color: #2a2d35; }
                        </span>-->
                    </div>
                </li>
            </ul>
        </nav>
    </aside>
    <!-- End navigation-->


    <!-- Main content-->
    <section class="content">
        <router-view></router-view>
    </section>

    <!-- End main content-->
</section>
</template>

<style>
</style>

<script>
import EngineClient from '../js/EngineClient.js';

export default {
    data() {
        return {
            version: undefined,
            engineClient: {}
        }
    },

    created() {
        engineClient = new EngineClient();
        window.useReasoner=false;
    },

    attached() {
        engineClient.getConfig((r, e) => { this.version=(r == null ? 'error' : r['project.version']) });
    }
}
</script>
