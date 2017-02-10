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
    <div id="myModal" class="modal">
        <div class="modal-content">
            <div class="close-cross"><span class="close">&times;</span></div>
            <div class="head">
                <h5 class="modal-title"><i style="font-size:35px;" class="pe page-header-icon pe-7s-rocket"></i>&nbsp; Join our GRAKN.AI Community &nbsp;<i style="font-size:35px;" class="pe page-header-icon pe-7s-rocket"></i></h5>
            </div>
            <div class="content">
                <p class="signup-text">We'd like to invite you to join our Slack and Discussion community.</p>
                <p class="signup-text">As an open source technology, your support means a lot to us!</p>
                <div class="panel">
                    <form @submit.prevent="validateBeforeSubmit" v-if="!formSubmitted">
                        <div class="form-group">
                            <div class="same-line">
                                <label class="label">First Name</label>
                                <input v-model="credentials.name" name="name" v-validate:name.initial="'required|alpha'" :class="{'input': true, 'is-danger': errors.has('name') }" type="text" class="form-control">
                            </div>
                            <span v-show="errors.has('name')" class="help-block"> <i v-show="errors.has('name')" class="fa fa-warning"></i>&nbsp;{{ errors.first('name') }}</span>
                        </div>
                        <div class="form-group">
                            <div class="same-line">
                                <label class="label">Last Name</label>
                                <input v-model="credentials.surname" name="lastname" v-validate:lastname.initial="'required|alpha'" :class="{'input': true, 'is-danger': errors.has('lastname') }" type="text" class="form-control">
                            </div>
                            <span v-show="errors.has('lastname')" class="help-block"><i v-show="errors.has('lastname')" class="fa fa-warning"></i>&nbsp;{{ errors.first('lastname') }}</span>
                        </div>
                        <div class="form-group">
                            <div class="same-line">
                                <label class="label">Email</label>
                                <input v-model="credentials.email" name="email" v-validate:email.initial="'required|email'" :class="{'input': true, 'is-danger': errors.has('email') }" type="text" class="form-control">
                            </div>
                            <span v-show="errors.has('email')" class="help-block"><i v-show="errors.has('email')" class="fa fa-warning"></i>&nbsp;{{ errors.first('email') }}</span>
                        </div>
                        <button class="btn" type="submit">Yes, count me in! :)</button>
                    </form>
                </div>
            </div>
        </div>

    </div>
    <div id="myModalSuccess" class="modal">
        <div class="modal-content">
            <div class="close-cross"><span class="close">&times;</span></div>
            <div class="head">
                <h5 class="modal-title"><i style="font-size:35px;" class="pe page-header-icon pe-7s-rocket"></i>&nbsp; Join our GRAKN.AI Community &nbsp;<i style="font-size:35px;" class="pe page-header-icon pe-7s-rocket"></i></h5>
            </div>
            <div class="content">
                <div class="panel">
                    <div class="modal-body">
                        <p class="signup-text">Thank you for joining us!</p>
                        <p class="signup-text">We already like you, very much! <i class="fa fa-smile-o"></i></p>
                    </div>
                </div>
            </div>
        </div>

    </div>
</div>
</template>

<style scoped>
.head {
    margin-bottom: 20px;
}

.close-cross {
    display: flex;
    justify-content: flex-end;
    width: 100%;
}

.close-cross span {
    color: #0f0f0f;
}

.same-line {
    display: flex;
    justify-content: flex-start;
    align-items: center;
}

label {
    width: 90px;
}

.signup-text {
    margin-bottom: 15px;
}

button {
    margin-top: 35px;
    margin-left: auto;
    margin-right: auto;
}

.form-group {
    display: flex;
    flex-direction: column;
    margin-bottom: 15px;
}

.panel {
    margin-top: 10px;
}

input {
    background-color: #0f0f0f;
    border: 0px;
    border-bottom: 1px solid #00eca2;
    color: white;
    outline: none;
    display: inline-flex;
    flex: 2;
    font-size: 100%;
}


/* The Modal (background) */

.modal {
    display: none;
    /* Hidden by default */
    position: fixed;
    /* Stay in place */
    z-index: 10;
    /* Sit on top */
    padding-top: 100px;
    /* Location of the box */
    left: 0;
    top: 0;
    width: 100%;
    /* Full width */
    height: 100%;
    /* Full height */
    overflow: auto;
    /* Enable scroll if needed */
    background-color: rgb(0, 0, 0);
    /* Fallback color */
    background-color: rgba(0, 0, 0, 0.3);
    /* Black w/ opacity */
}


/* Modal Content */

.modal-content {
    position: relative;
    background-color: #0f0f0f;
    margin: auto;
    padding: 15px;
    width: 30%;
    box-shadow: 0 4px 8px 0 rgba(0, 0, 0, 0.2), 0 6px 20px 0 rgba(0, 0, 0, 0.19);
    display: flex;
    flex-direction: column;
    align-items: center;
    -webkit-animation-name: animatetop;
    -webkit-animation-duration: 0.4s;
    animation-name: animatetop;
    animation-duration: 0.4s
}


/* Add Animation */

@-webkit-keyframes animatetop {
    from {
        top: 300px;
        opacity: 0
    }
    to {
        top: 0;
        opacity: 1
    }
}

@keyframes animatetop {
    from {
        top: 300px;
        opacity: 0
    }
    to {
        top: 0;
        opacity: 1
    }
}


/* The Close Button */

.close {
    color: white;
    float: right;
    font-size: 28px;
    font-weight: bold;
}

.close:hover,
.close:focus {
    color: #000;
    text-decoration: none;
    cursor: pointer;
}
</style>

<script>
import User from '../../js/User.js'
import EngineClient from '../../js/EngineClient.js';


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
            // Get the modal
            var modal = document.getElementById('myModal');

            // Get the button that opens the modal
            var btn = document.getElementById("myBtn");

            // Get the <span> element that closes the modal
            var span = document.getElementsByClassName("close")[0];

            // When the user clicks the button, open the modal
            btn.onclick = function() {
                modal.style.display = "block";
            }

            // When the user clicks on <span> (x), close the modal
            span.onclick = function() {
                modal.style.display = "none";
            }

            // When the user clicks anywhere outside of the modal, close it
            window.onclick = function(event) {
                if (event.target == modal) {
                    modal.style.display = "none";
                }
            }
        });
    },

    methods: {
        /*
         * Listener methods on emit from GraqlEditor
         */
        validateBeforeSubmit(e) {
            this.$validator.validateAll();
            if (!this.errors.any()) {
                this.signUp();
            }
        },
        signUp() {
            this.formSubmitted = true
            EngineClient.sendInvite(this.credentials, this.signupComplete);
        },
        signupComplete(response) {
            var modal = document.getElementById('myModal');
            modal.style.display = "none";
            modal = document.getElementById('myModalSuccess');
            modal.style.display = "block";
            // When the user clicks anywhere outside of the modal, close it
            window.onclick = function(event) {
                if (event.target == modal) {
                    modal.style.display = "none";
                }
            }
            this.formSubmitted = false;
            this.credentials.name = undefined;
            this.credentials.surname = undefined;
            this.credentials.email = undefined;
        }


    }
}
</script>
