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
along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>. -->


<template>
<div>
    <button @click="loadFavQueries" class="btn btn-default console-button"><i class="pe-7s-search"></i>
  </button>
    <transition name="slide-fade">
        <div v-if="showFavourites" style="margin-bottom: 0px; margin-top: 20px;" class="dropdown-content">
            <div class="panel panel-filled" id="queriesDropDown">
                <div class="dd-item" v-for="(value,key) in favouriteQueries">
                    <div @click="emitTypeQuery(value)" class="dd-handle"><span class="list-key">{{key}}:</span>
                        <span> {{value}}</span>
                    </div>
                </div>
            </div>
        </div>
    </transition>
</div>
</template>

<style scoped>
.dropdown-content{
  position: absolute;
  right:5px;
  top:25px;
  z-index:10;
}

</style>

<script>
import FavQueries from '../js/FavQueries.js'

export default {
    name: "favQueriesList",
    data() {
        return {
            favouriteQueries: {},
            showFavourites: false
        }
    },

    created() {},

    mounted: function() {
        this.$nextTick(function() {
            // code for previous attach() method.
        });
    },

    methods: {
        loadFavQueries() {
            if (!this.showFavourites) {
                this.favouriteQueries = FavQueries.getFavQueries();
                this.showFavourites = true;
            } else {
                this.showFavourites = false;
            }
        },
        emitTypeQuery(query) {
            this.$emit('type-query', query);
        },
    }
}
</script>
