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
<div class="modal fade" id="signupModal" tabindex="-1" role="dialog" aria-hidden="true" style="display: none;">
    <div class="modal-dialog">
        <div class="modal-content">
            <div class="modal-header text-center" id="singupModalHeader">
                <h5 class="modal-title"><i style="font-size:35px;" class="pe page-header-icon pe-7s-rocket"></i>&nbsp; Join our GRAKN.AI Community &nbsp;<i style="font-size:35px;" class="pe page-header-icon pe-7s-rocket"></i></h5>
            </div>
            <div class="modal-body">
              <p class="signup-text">Hey! We're an open-source technology. We get our funding based on adoption.</p>
              <p class="signup-text">It would mean a lot to us if you would like to join our community and support us! :)</p>
              <div class="panel panel-filled">
                  <div class="panel-body in-line">
                          <div class="form-group">
                              <label class="control-label" for="name">First Name</label>
                              <input type="text" v-model="credentials.name" required value="" class="form-control">
                          </div>
                          <div class="form-group">
                              <label class="control-label" for="surname">Last Name</label>
                              <input type="text" v-model="credentials.surname"  required value="" class="form-control">
                          </div>
                          <div class="form-group">
                              <label class="control-label" for="email">E-Mail</label>
                              <input type="text" v-model="credentials.email" required value="" class="form-control">
                          </div>
                          <div style="float:right;">
                              <button class="btn btn-info" @click="signUp()">Yes, count me in! :)</button>
                          </div>
                  </div>
              </div>
            </div>
        </div>
    </div>
</div>
</template>

<style>
#signupModal .modal-body{
  padding-top:20px 40px;
}
/*
#signupModal .modal-header{
  border-color: white;
  background-color: #87A0B2;
}*/
#signupModal .btn-info{
  color: white;
}
.signup-text{
  color:white;
  font-size: 120%;
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
              email:undefined,
              community:undefined
          }
        }
    },

    created() {
    },

    mounted: function() {
        this.$nextTick(function() {

        });
    },

    methods: {
        /*
         * Listener methods on emit from GraqlEditor
         */

         signUp(){
          this.credentials.community="SlackChannel";
          EngineClient.sendInvite(this.credentials,this.sendMailChimp);

        },
        sendMailChimp(){
          this.credentials.community="MailChimp";
          EngineClient.sendInvite(this.credentials,this.signupComplete);
        },
        signupComplete(){
          $('#signupModal').modal('hide');
        }


    }
}
</script>
