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
<div id="addCurrentQuery">
    <span @click="showToolTipFn" class="pe-7s-plus"></span>
    <transition name="slide-fade">
        <div v-if="showToolTip" class="arrow_box">
            <div class="row"><p>Add query to favourites</p></div>
            <div class="row">
            <form class="form-inline">
                <input type="text" class="form-control input-sm query-name" v-model="currentQuery.name" placeholder="input a query name">
                <input type="text" class="form-control input-sm query-value" v-model="currentQuery.value" disabled>
                <button @click="addQuery" class="btn btn-sm btn-default">Save<i
                        class="pe-7s-angle-right-circle"></i></button>
            </form>
          </div>
        </div>
    </transition>
</div>
</template>

<style scoped>
.query-name{
  width:130px;
}
.query-value{
  width:270px;
}

input {
    color: white;
}

.btn-default {
    color: white;
}

.form-group {
    margin-bottom: 5px;
}

form, p {
    float: left;
    margin-left:10px;
}

.slide-fade-enter-active {
    transition: all .6s ease;
}

.slide-fade-leave-active {
    transition: all .3s cubic-bezier(1.0, 0.5, 0.8, 1.0);
}

.slide-fade-enter,
.slide-fade-leave-active {
    transform: translateX(10px);
    opacity: 0;
}

div>span {
    color: white;
    font-size: 20px;
    cursor: pointer;
}

#addCurrentQuery {
    position: relative;
    display: inline-block;
}

.arrow_box:after,
.arrow_box:before {
    bottom: 100%;
    left: 95%;
    border: solid transparent;
    content: " ";
    height: 0;
    width: 0;
    position: absolute;
    pointer-events: none;
}

.arrow_box:after {
    position: absolute;
    border-color: rgba(136, 183, 213, 0);
    border-bottom-color: rgba(132, 135, 148, 0.8);
    border-width: 8px;
}

.arrow_box {
    width: 490px;
    padding: 5px 10px;
    background-color: rgba(132, 135, 148, 0.8);
    color: #fff;
    text-align: center;
    border-radius: 4px;
    position: absolute;
    z-index: 1;
    top: 120%;
    right: -25%;
}
</style>

<script>
import FavQueries from '../js/FavQueries.js'

export default {
    name: "addQueryButton",
    props: ['codeMirror'],
    data() {
        return {
            showToolTip: false,
            currentQuery: {
                name: undefined,
                value: undefined
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
        /*r
         * Listener methods on emit from GraqlEditor
         */
        showToolTipFn() {
            if (!this.showToolTip) {
                this.currentQuery.value = this.codeMirror.getValue();
                if (this.currentQuery.value === "") return;
            }
            this.showToolTip = !this.showToolTip;
        },
        addQuery() {
            FavQueries.addFavQuery(this.currentQuery);
            this.showToolTip = false;
        }

    }
}
</script>
