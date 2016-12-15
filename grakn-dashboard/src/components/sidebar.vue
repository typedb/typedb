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
            <li class="nav-category">
                Actions
            </li>
            <li><a @click="openKeySpaces()">Keyspaces</a></li>
            <li v-show="isUserAuth" @click="logout()"><a href="#">Log Out</a></li>
        </ul>
    </nav>
    <a @click="openSignUp()">
        <div class="join-community">
            Join our Community
        </div>
    </a>
</aside>
</template>

<style>
.nav-category {
    font-size: 120%;
}

.join-community {
    width: 150px;
    height: 180px;
    padding-top: 4px;
    text-align: center;
    font-weight: 200;
    margin-top: 20px;
    font-size: 150%;
    height: 50px;
    line-height: 20px;
    color: #848c94;
    cursor: pointer;
}

.join-community:hover {
    text-decoration: underline;
    text-decoration-color: #FF8D7D;
}
</style>

<script>
import EngineClient from '../js/EngineClient.js';
import User from '../js/User.js';


export default {
    data: function() {
        return {
            version: undefined,
            isUserAuth: User.isAuthenticated()
        }
    },
    created: function() {},
    mounted: function() {
        this.$nextTick(function() {
            EngineClient.getConfig((r, e) => {
                this.version = (r == null ? 'error' : r['project.version'])
            });
            if (!User.getModalShown()) $('#signupModal').modal('show');
        })
    },
    methods: {
        openKeySpaces() {
            $('#keySpacesModal').modal('show');
        },
        openSignUp() {
            $('#signupModal').modal('show');
        },
        logout() {
            User.logout();
            this.$router.push("/login");
        }
    }
}
</script>
