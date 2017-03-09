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
    <!-- Main content-->
    <section class="content login">
        <div class="container-center animated slideInDown">
            <div class="view-header">
                <div class="header-icon">
                    <i class="pe page-header-icon pe-7s-unlock"></i>
                </div>
                <div class="header-title">
                    <h3>GRAKN.AI - Log In</h3>
                    <small>
                        Enter your credentials to access the Grakn portal.
                    </small>
                </div>
            </div>

            <div class="panel panel-filled">
                <div class="panel-body">
                        <div class="form-group">
                            <label class="control-label" for="username">Username</label>
                            <input type="text" v-model="credentials.username" title="Please enter you username" required value="" class="form-control">
                        </div>
                        <div class="form-group">
                            <label class="control-label" for="password">Password</label>
                            <input type="password" v-model="credentials.password" title="Please enter your password" required value="" class="form-control">
                        </div>
                        <div>
                            <button class="btn btn-accent" @click="submit()">Log In</button>
                        </div>
                </div>
            </div>

        </div>
    </section>
    <!-- End main content-->

</section>
</template>

<style>
.content.login {
    margin-left: 0px;
}
</style>

<script>
import User from '../js/User.js'

export default {
    name: "LoginPage",
    data() {
        return {
            credentials: {
                username: undefined,
                password: undefined
            }
        }
    },

    created() {},

    mounted: function() {
        this.$nextTick(function() {
            // code for previous attach() method.
        });
    },

    methods: {
        /*
         * Listener methods on emit from GraqlEditor
         */
        submit() {
            User.newSession(this.credentials).then(this.onLoginResponse);
        },
        onLoginResponse(res, err) {
            if (res != null) {
                User.setAuthToken(res);
                this.$router.push("/");
            } else {
                //implement promise so that we can send back the failure to the login.vue controller?
            }
        },

    }
}
</script>
