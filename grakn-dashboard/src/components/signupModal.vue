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
<div>
    <div class="modal fade" id="signupModal" tabindex="-1" role="dialog" aria-hidden="true" style="display: none;">
        <div class="modal-dialog">
            <div class="modal-content">
                <div class="modal-header text-center" id="singupModalHeader">
                    <h5 class="modal-title"><i style="font-size:35px;" class="pe page-header-icon pe-7s-rocket"></i>&nbsp; Join our GRAKN.AI Community &nbsp;<i style="font-size:35px;" class="pe page-header-icon pe-7s-rocket"></i></h5>
                </div>
                <div class="modal-body">
                    <p class="signup-text">We'd like to invite you to join our Slack and Discussion community.</p>
                    <p class="signup-text">As an open source technology, your support means a lot to us!</p>
                    <div class="panel panel-filled">
                        <div class="panel-body in-line">
                            <form @submit.prevent="validateBeforeSubmit" v-if="!formSubmitted">
                                <div class="form-group">
                                    <label class="label">First Name</label>
                                    <input v-model="credentials.name" v-validate.initial="name" data-vv-rules="required|alpha" :class="{'input': true, 'is-danger': errors.has('name') }" type="text" class="form-control">
                                    <span v-show="errors.has('name')" class="help-block"> <i v-show="errors.has('name')" class="fa fa-warning"></i>&nbsp;{{ errors.first('name') }}</span>
                                </div>
                                <div class="form-group">
                                    <label class="label">Last Name</label>
                                    <input v-model="credentials.surname" v-validate.initial="lastname" data-vv-rules="required|alpha" :class="{'input': true, 'is-danger': errors.has('lastname') }" type="text" class="form-control">
                                    <span v-show="errors.has('lastname')" class="help-block"><i v-show="errors.has('lastname')" class="fa fa-warning"></i>&nbsp;{{ errors.first('lastname') }}</span>
                                </div>
                                <div class="form-group">
                                    <label class="label">Email</label>
                                    <input v-model="credentials.email" v-validate.initial="email" data-vv-rules="required|email" :class="{'input': true, 'is-danger': errors.has('email') }" type="text" class="form-control">
                                    <span v-show="errors.has('email')" class="help-block"><i v-show="errors.has('email')" class="fa fa-warning"></i>&nbsp;{{ errors.first('email') }}</span>
                                </div>
                                <button class="btn btn-primary btn-block" type="submit">Yes, count me in! :)</button>
                            </form>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
    <div class="modal fade" id="signupSuccess" tabindex="-1" role="dialog" aria-hidden="true" style="display: none;">
        <div class="modal-dialog">
            <div class="modal-content">
                <div class="modal-header text-center" id="singupModalHeader">
                    <h5 class="modal-title"><i style="font-size:35px;" class="pe page-header-icon pe-7s-rocket"></i>&nbsp; Join our GRAKN.AI Community &nbsp;<i style="font-size:35px;" class="pe page-header-icon pe-7s-rocket"></i></h5>
                </div>
                <div class="modal-body">
                    <p class="signup-text">Thank you for joining us!</p>
                    <p class="signup-text">We already like you, very much! <i class="fa fa-smile-o"></i></p>
                </div>
                <div class="modal-footer">
                    <button type="button" class="btn btn-default" data-dismiss="modal">Done</button>
                </div>
            </div>
        </div>
    </div>
</div>
</template>

<style scoped>
#signupModal .modal-body {
    padding-top: 20px 40px;
}
.panel-filled{
  margin-top: 15px;
}
button{
  color:white;
}

#signupModal .btn-info {
    color: white;
}

.signup-text {
    color: white;
    font-size: 120%;
    text-align:center;
}
</style>

<script>
import User from '../js/User.js'
import EngineClient from '../js/EngineClient.js';


export default {
    name: "SignUpModal",
    data() {
        return {
            credentials: {
                name: undefined,
                surname: undefined,
                email: undefined
            },
            formSubmitted: false
        }
    },

    created() {},

    mounted: function() {
        this.$nextTick(function() {
            User.setModalShown(true);
        });
    },

    methods: {
        /*
         * Listener methods on emit from GraqlEditor
         */
        validateBeforeSubmit(e) {
            this.$validator.validateAll();
            if (!this.errors.any()) {
                this.signUp()
            }
        },
        signUp() {
            this.formSubmitted = true
            EngineClient.sendInvite(this.credentials, this.signupComplete);
        },
        signupComplete(response) {
            $('#signupModal').modal('hide');
            $('#signupSuccess').modal('show');
            this.formSubmitted = false;
            this.credentials.name=undefined;
            this.credentials.surname=undefined;
            this.credentials.email=undefined;
        }


    }
}
</script>
