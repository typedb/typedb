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
        <!-- <div class="panel-body graph-panel-body">
            <div id="graph-div" ref="graph"></div>
            <div class="panel panel-filled panel-c-accent properties-tab" id="list-resources-tab">
                <div class="panel-heading">
                    <div class="panel-tools">
                        <a class="panel-close" @click="closeConfigPanel"><i class="fa fa-times"></i></a>
                    </div>
                    <h4><i id="graph-icon" class="pe page-header-icon pe-7s-share"></i>{{selectedNodeLabel}}</h4>
                </div>
                <div class="panel-body">
                    <div class="properties-list">
                        <span>Node:</span>
                        <div class="node-properties">
                            <div class="dd-item" v-for="(value, key) in allNodeOntologyProps">
                                <div><span class="list-key">{{key}}:</span> {{value}}</div>
                            </div>
                        </div>
                        <span v-show="numOfResources>0">Resources:</span>
                        <div class="dd-item" v-for="(value,key) in allNodeResources">
                            <div class="dd-handle" @dblclick="addResourceNodeWithOwners(value.link)"><span class="list-key">{{key}}:</span>
                                <a v-if="value.href" :href="value.label" style="word-break: break-all;" target="_blank">{{value.label}}</a>
                                <span v-else> {{value.label}}</span>
                            </div>
                        </div>
                        <span v-show="numOfLinks>0">Links:</span>
                        <div class="dd-item" v-for="(value, key) in allNodeLinks">
                            <div class="dd-handle"><span class="list-key">{{key}}:</span> {{value}}</div>
                        </div>
                    </div>
                </div>
            </div>
        </div> -->
        <!-- --------------------------- -->
        <div v-if="showFavourites" style="margin-bottom: 0px; margin-top: 20px;" class="dropdown-content">
            <div class="panel panel-filled" id="queriesDropDown">
                <div class="panel-heading">
                    <div class="panel-tools">
                        <a class="panel-close" @click="closeFavQueriesList"><i class="fa fa-times"></i></a>
                    </div>
                    <h4><i id="graph-icon" class="pe page-header-icon pe-7s-share"></i>Fav queries</h4>
                </div>
                <div class="panel-body">
                    <div class="dd-item row" v-for="(query,index) in favouriteQueries">
                        <div class="full-query tooltip-query col-sm-8 dd-handle" @click="emitTypeQuery(query.value)">
                            <span class="list-key"> {{query.name}}</span>
                            <span class="tooltiptext"> {{query.value}}</span>
                        </div>
                        <div class="col-sm-2">
                            <button class="btn btn-sm btn-danger" @click="removeFavQuery(index,query.name)">Delete</button>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </transition>
</div>
</template>

<style scoped>
#queriesDropDown {}


/* Tooltip text */

.panel-filled {
    background-color: rgba(68, 70, 79, 1);
}

.tooltip-query .tooltiptext {
    visibility: hidden;
    width: 120px;
    background-color: black;
    color: #fff;
    text-align: center;
    padding: 5px 0;
    border-radius: 6px;
    position: absolute;
    z-index: 5;
    top: -10px;
    right: 105%;
}


/* Show the tooltip text when you mouse over the tooltip container */

.tooltip-query:hover .tooltiptext {
    visibility: visible;
}

.dropdown-content {
    position: absolute;
    right: 0px;
    top: 80px;
    z-index: 10;
}

.full-query {
    width: 200px;
    display: inline-flex;
}

.btn-danger {
    margin-top: 10px;
}
</style>

<script>
import FavQueries from '../js/FavQueries.js'
import _ from 'underscore';


export default {
    name: "favQueriesList",
    data() {
        return {
            favouriteQueries: [],
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
                this.favouriteQueries = this.objectToArray(FavQueries.getFavQueries());
                this.showFavourites = true;
            }
        },
        closeFavQueriesList() {
            this.showFavourites = false;
        },
        objectToArray(object) {
            return Object.keys(object).reduce(
                (r, k) => {
                    r.push({
                        name: k,
                        value: object[k]
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
        }
    }
}
</script>
