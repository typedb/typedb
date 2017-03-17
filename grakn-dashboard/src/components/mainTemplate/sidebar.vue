<template>
<transition name="slide-fast-right-fade">
    <aside class="z-depth-1">
        <nav class="side-vert-flex">
            <ul class="nav">
                <router-link tag="li" to="/graph" class="link">
                    <a class="noselect">Graph</a>
                </router-link>
                <router-link tag="li" to="/console" class="link">
                    <a class="noselect">Console</a>
                </router-link>
                <router-link tag="li" to="/tasks" class="link">
                    <a class="noselect">Tasks</a>
                </router-link>
                <router-link tag="li" to="/config" class="link">
                    <a class="noselect">Config</a>
                </router-link>
                <li class="link"><a target="_blank" href="https://grakn.ai/pages/index.html" class="noselect">Documentation</a></li>
                <li v-show="isUserAuth" @click="logout()"><a href="#">Log Out</a></li>
            </ul>
        </nav>
        <div class="divide"></div>
        <div class="green-link noselect" id="myBtn">
            Join Community
        </div>
        <div class="green-link">
            <a target="_blank" href="https://github.com/graknlabs/grakn" class="noselect">Star on GitHub</a>
        </div>
    </aside>
</transition>
</template>

<style scoped>
aside {
    position: absolute;
    background-color: #0f0f0f;
    left: 10px;
    top: 10px;
    width: 160px;
    z-index: 2;
    padding: 10px;
}

.divide{
  border-bottom: 1px solid #606060;
  min-height: 5px;
  margin-bottom: 5px;
}

.green-link {
    color: #00eca2;
    padding: 15px;
    cursor: pointer;
}

.green-link a{
  color: #00eca2;
}

.side-vert-flex {
    display: flex;
    flex-direction: column;
}

.link:hover a {
    color: #00eca2;
}

.link.active {
    border-left: 4px solid #00eca2;
    padding-left: 11px;
}

ul.nav {
    display: flex;
    flex-direction: column;
}

li {
    padding: 15px;
}

.nav-category {
    font-size: 120%;
    font-weight: bold;
}
</style>

<script>
import User from '../../js/User.js';


export default {
    props: ['showSideBar'],
    data: function() {
        return {
            isUserAuth: User.isAuthenticated(),
        }
    },
    created: function() {},
    mounted: function() {
        this.$nextTick(function() {
        })
    },
    methods: {
        openSignUp() {
            $('.modal-wrapper').toggleClass('open');
        },
        logout() {
            User.logout();
            this.$router.push("/login");
        }
    }
}
</script>
