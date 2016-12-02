<template>
<aside class="navigation">
    <div class="brand" href="/">
        GRAKN.AI
        <span>{{version}}</span>
    </div>
    <nav>
        <ul class="nav luna-nav">
            <li class="nav-category">
                Pages
            </li>
            <router-link tag="li" to="/graph">
                <a>Graph</a>
            </router-link>
            <router-link tag="li" to="/console">
                <a>Console</a>
            </router-link>
            <router-link tag="li" to="/config">
                <a>Config</a>
            </router-link>
            <li>
                <a target="_blank" href="https://grakn.ai/pages/index.html">Documentation</a>
            </li>
            <li>
                <a href="#uielements" data-toggle="collapse" aria-expanded="false" class="collapsed nav-category">
                                          Actions<span class="sub-nav-icon"> <i class="stroke-arrow"></i> </span>
                                      </a>
                <ul id="uielements" class="nav nav-second collapse" aria-expanded="false" style="height: 0px;">
                    <li><a href="#" @click="openKeySpaces()">Keyspaces</a></li>
                    <li v-show="isUserAuth" @click="logout()"><a href="#">Log Out</a></li>
                </ul>
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
</template>

<style>
.nav-category {
    font-size: 120%;
}
</style>

<script>
import EngineClient from '../js/EngineClient.js';
import User from '../js/User.js';


export default {
    data: function() {
        return {
            version: undefined,
            engineClient: {},
            isUserAuth: User.isAuthenticated()
        }
    },
    created: function() {
        this.engineClient = new EngineClient();
        window.useReasoner = false;
    },
    mounted: function() {
        this.$nextTick(function() {
            this.engineClient.getConfig((r, e) => {
                this.version = (r == null ? 'error' : r['project.version'])
            });

        })
    },
    methods: {
        openKeySpaces() {
            $('#keySpacesModal').modal('show');
        },
        logout() {
            User.logout();
            this.$router.push("/login");
        }
    }
}
</script>
