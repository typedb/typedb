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
<div class="modal fade" id="keySpacesModal" tabindex="-1" role="dialog" aria-hidden="true" style="display: none;">
    <div class="modal-dialog modal-sm">
        <div class="modal-content">
            <div class="modal-header text-center">
                <h5 class="modal-title">Select keyspace &nbsp;<i style="font-size:35px;" class="pe page-header-icon pe-7s-server"></i></h5>
            </div>
            <div class="modal-body">
                <div class="properties-list">
                  <p>Available keyspaces</p>
                    <ul class="dd-list">
                        <li class="dd-item" v-for="name in keyspaces" v-bind:class="{'li-active':(currentKeyspace===name)}">
                            <div class="dd-handle" @click="setKeySpace(name)">{{name}}</div>
                        </li>
                    </ul>
                </div>
            </div>
            <div class="modal-footer">
                <button type="button" class="btn btn-default" data-dismiss="modal">Done</button>
            </div>
        </div>
    </div>
</div>
</template>

<style>

</style>
<script>
import User from '../js/User.js'
import EngineClient from '../js/EngineClient.js';


export default {
    name: "KeySpaceModal",
    data() {
        return {
            keyspaces: [],
            engineClient:{},
            currentKeyspace:User.getCurrentKeySpace()
        }
    },

    created() {
      this.engineClient = new EngineClient();
    },

    mounted: function() {
        this.$nextTick(function() {
            this.engineClient.fetchKeyspaces((res,err) => {
                let list = JSON.parse(res);
                let currentKeyspace = User.getCurrentKeySpace();
                for (let i = 0; i < list.length; i++) {
                    this.keyspaces.push(list[i]);
                }
            });
        });
    },

    methods: {
        /*
         * Listener methods on emit from GraqlEditor
         */
        submit() {
            User.newSession(this.credentials);
        }
        ,
        setKeySpace(name){
          User.setCurrentKeySpace(name);
          this.currentKeyspace=name;
        }

    }
}
</script>
