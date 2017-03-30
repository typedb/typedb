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
<div v-if="currentQuery" id="addCurrentQuery">
    <span @click="showToolTipFn" class="pe-7s-plus"></span>
    <transition name="slide-fade">
        <div v-if="showToolTip" class="tooltip-arrow-box">
            <div>
                <p>Save current query</p>
            </div>
            <div class="tooltip-body">
                <div><input type="text" class="form-control query-name" v-model="currentQueryName" placeholder="query name" v-focus></div>
                <div class="language-graql-wrapper">
                    <div class="language-graql" v-html="highlightedQuery"></div>
                </div>
                <div><button @click="addQuery" class="btn btn-default" :disabled="currentQueryName.length==0">Save</button></div>
            </div>
        </div>
    </transition>
</div>
</template>

<style scoped>
/*TODO: force language-graql to stay on 1 line and scroll horizontally*/
.query-name {
    width: 130px;
}

.tooltip-body{
  display: flex;
  flex-direction: row;
  align-items: center;
}

.language-graql {
    background-color: #494b54;
    padding: 7px 13px;
    margin: 0px;
    width: 480px;
    text-align: left;
}

.row>div {
    display: inline-block;
    margin-left: 5px;
}

.btn-default {
    color: white;
    border: 1px solid white;
}
.language-graql-wrapper{
  width: 480px;
  background-color: #494b54;
  overflow: scroll;
  -moz-user-select: none;
  -ms-overflow-style: none;
  overflow: -moz-scrollbars-none;
}

.language-graql-wrapper::-webkit-scrollbar {
    display: none;
}


/*Transition for the arrow box*/

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


/*Icon with plus sign*/

div>span {
    color: #56C0E0;
    font-size: 25px;
    cursor: pointer;
}


/*Tooltip positioning*/

#addCurrentQuery {
  display: inline-flex;
  margin: auto;
}

.tooltip-arrow-box:after,
.tooltip-arrow-box:before {
    bottom: 100%;
    left: 96%;
    border: solid transparent;
    content: " ";
    height: 0;
    width: 0;
    position: absolute;
    pointer-events: none;
}

.tooltip-arrow-box:after {
    position: absolute;
    border-color: rgba(136, 183, 213, 0);
    border-bottom-color: rgba(38, 41, 48, 1);
    border-width: 8px;
}


.tooltip-arrow-box {
    width: 700px;
    padding: 5px 10px;
    background-color: rgba(38, 41, 48, 1);
    color: #fff;
    text-align: center;
    border-radius: 4px;
    position: absolute;
    z-index: 1;
    top: 120%;
    right: -25%;
    display: flex;
    flex-direction: column;
}
</style>

<script>
import FavQueries from '../../js/FavQueries.js'
import Prism from 'prismjs';
import * as PGraqlLang from '../../js/prismGraql.js';


export default {
    name: "addQueryButton",
    props: ['currentQuery'],
    data() {
        return {
            showToolTip: false,
            currentQueryName: "",
            highlightedQuery: undefined
        }
    },

    created() {},
    directives: {
        //Registering local directive to always force focus on query-name input
        focus: {
            inserted: function(el) {
                el.focus()
            }
        }
    },
    mounted: function() {
        this.$nextTick(function() {
            // code for previous attach() method.
        });
    },

    methods: {
        showToolTipFn() {
            if (!this.showToolTip) {
                if (this.currentQuery === "") return;
                this.highlightedQuery = Prism.highlight(this.currentQuery, PGraqlLang.default);
            }
            this.showToolTip = !this.showToolTip;
        },
        addQuery() {
            FavQueries.addFavQuery(this.currentQueryName, this.currentQuery);
            toastr.success("New query saved!");
            this.showToolTip = false;
            this.currentQueryName = "";
            this.$emit('new-query-saved');
        }

    }
}
</script>
