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
    <transition name="slide-fade">
        <div v-if="showFavourites" style="margin-bottom: 0px; margin-top: 20px;" class="dropdown-content">
            <div class="panel panel-filled" id="queriesDropDown" v-resize>
                <div class="panel-heading">
                    <div class="panel-tools">
                        <a class="panel-close" @click="closeFavQueriesList"><i class="fa fa-times"></i></a>
                    </div>
                    <h4><i class="page-header-icon fa fa-star-o"></i>Saved queries</h4>
                </div>
                <div class="panel-body">
                    <div class="dd-item row" v-for="(query,index) in favouriteQueries">
                        <div class="full-query tooltip-query col-sm-8 dd-handle" @click="emitTypeQuery(query.value)">
                            <span class="list-key" style="word-break: break-all;"> {{query.name}}</span>
                            <span class="tooltiptext" style="word-break: break-all;"> {{query.value}}</span>
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
/*#queriesDropDown .panel-body {
  overflow: scroll;
}*/


/* Tooltip text */

.panel-filled {
    background-color: rgba(68, 70, 79, 1);
}

.panel-heading{
  padding:5px 10px;
}

.page-header-icon{
  font-size: 20px;
  float:left;
  margin-right: 10px;
}

.tooltip-query .tooltiptext {
    visibility: hidden;
    width: 300px;
    background-color: rgba(38, 41, 48, 1);
    color: #fff;
    text-align: center;
    padding: 5px 10px;
    border-radius: 3px;
    position: absolute;
    z-index: 5;
    top: 0px;
    right: 110%;
    text-align: left;
    overflow: scroll;
    -moz-user-select: none;
    -ms-overflow-style: none;
    overflow: -moz-scrollbars-none;
}


/* Show the tooltip text when you mouse over the tooltip container */

.tooltip-query:hover .tooltiptext {
    visibility: visible;
}

.dropdown-content {
    position: absolute;
    right: 0px;
    top: 78px;
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
    directives:{
      resize:{
        inserted:function(el){
          let divHeight = window.innerHeight-$('#graph-div').offset().top - 20;
          // code for previous attach() method.
          $(el).height(divHeight);
          $('#queriesDropDown .panel-body').height(divHeight - 85);

        }
      }
    },

    mounted: function() {
        this.$nextTick(function() {

        });
    },

    methods: {
        loadFavQueries() {
            if (!this.showFavourites) {
                this.closeConfigPanel();
                this.favouriteQueries = this.objectToArray(FavQueries.getFavQueries());
                this.showFavourites = true;
            }
        },
        //TODO: move following function a method in a mixin to import.
        closeConfigPanel() {
            if ($('.properties-tab.active').hasClass('slideInRight')) {
                $('.properties-tab.active').removeClass('animated slideInRight');
                $('.properties-tab.active').fadeOut(300, () => {
                    this.nodeType = undefined;
                    this.allNodeProps = [];
                    this.selectedProps = [];
                });
                $('.properties-tab.active').removeClass('active');
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
                        value: object[k].replace("\n"," ")
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
