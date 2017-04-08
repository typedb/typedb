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
    <button @click="loadFavQueries" class="btn btn-default console-button"><i class="fa fa-star-o"></i>
  </button>
    <transition name="fade-in">
        <div v-if="showFavourites" class="dropdown-content">
            <div class="panel-heading">
                <div></div>
                <h4><i class="page-header-icon fa fa-star-o"></i>Saved queries</h4>
                <a @click="closeFavQueriesList"><i class="fa fa-times"></i></a>
            </div>
            <div class="panel-body" v-if="favouriteQueries.length">
                <div class="dd-item" v-for="(query,index) in favouriteQueries">
                    <div class="full-query">
                        <span class="list-key"> {{query.name}}</span>
                    </div>
                    <div class="line-buttons">
                        <button class="btn bold" @click="emitTypeQuery(query.value)">USE</button>
                        <button class="btn" @click="removeFavQuery(index,query.name)"><i class="fa fa-trash-o"></i></button>
                    </div>
                </div>
            </div>
            <div class="panel-body" v-else>
                <div class="dd-item">
                    <div class="no-saved">
                        No saved queries.
                    </div>
                </div>
            </div>

        </div>
    </transition>
</div>
</template>

<style scoped>
.panel-filled {
    background-color: rgba(68, 70, 79, 1);
}

.bold {
    font-weight: bold;
}

.no-saved {
    margin-bottom: 15px;
}

.fa-trash-o {
    font-size: 95%;
}

a {
    margin-left: auto;
}

.fa-times{
  cursor: pointer;
}

.panel-heading {
    padding: 5px 10px;
    display: flex;
    justify-content: space-between;
    align-items: center;
}

.page-header-icon {
    font-size: 20px;
    float: left;
    margin-right: 10px;
}

.panel-body {
    display: flex;
    flex-direction: column;
}

.dd-item {
    display: flex;
    align-items: center;
    margin-top: 5px;
    padding-left: 7px;
}

.list-key {
    display: inline-flex;
    flex: 1;
}

.line-buttons {
    display: inline-flex;
    align-items: center;
}

.dropdown-content {
    position: absolute;
    top: 100%;
    left: -40%;
    z-index: 2;
    margin-top: 5px;
    background-color: #0f0f0f;
}


/* Tooltip text */

.full-query .tooltiptext {
    visibility: hidden;
    width: 100%;
    background-color: rgba(38, 41, 48, 1);
    color: #fff;
    text-align: center;
    padding: 5px 10px;
    border-radius: 3px;
    position: absolute;
    z-index: 5;
    bottom: 120%;
    text-align: left;
    word-break: normal;
    overflow: scroll;
    -moz-user-select: none;
    -ms-overflow-style: none;
    overflow: -moz-scrollbars-none;
}


/* Show the tooltip text when you mouse over the tooltip container */

.full-query {
    display: inline-flex;
    position: relative;
    border-bottom: 1px solid #606060;
    padding-bottom: 5px;
    flex: 3;
    margin-right: 10px;
}
</style>

<script>
import FavQueries from '../../js/FavQueries.js'
import _ from 'underscore';


export default {
    name: "FavQueriesList",
    data() {
        return {
            favouriteQueries: [],
            showFavourites: false
        }
    },
    created() {},
    mounted: function() {
        this.$nextTick(function() {

        });
    },

    methods: {
        loadFavQueries() {
            if (!this.showFavourites) {
                this.favouriteQueries = this.objectToArray(FavQueries.getFavQueries());
                this.showFavourites = true;
            } else {
                this.showFavourites = false;
            }
        },
        refreshList() {
            this.favouriteQueries = this.objectToArray(FavQueries.getFavQueries());
        },
        closeFavQueriesList() {
            this.showFavourites = false;
        },
        objectToArray(object) {
            return Object.keys(object).reduce(
                (r, k) => {
                    r.push({
                        name: k,
                        value: object[k].replace("\n", " ")
                    });
                    return r;
                }, []);
        },
        removeFavQuery(index, queryName) {
            this.favouriteQueries.splice(index, 1);
            FavQueries.removeFavQuery(queryName);
        },
        emitTypeQuery(query) {
            this.$emit('type-query', query);
            this.showFavourites = false;
        }
    }
}
</script>
