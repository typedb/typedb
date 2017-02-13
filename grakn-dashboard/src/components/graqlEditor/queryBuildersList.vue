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
    <button @click="showList=!showList" class="btn btn-default console-button"><i class="fa fa-pencil"></i></button>
    <transition name="fade-in">
        <div v-if="showList" class="dropdown-content">
            <div class="panel-heading">
                <h4><i class="page-header-icon fa fa-pencil"></i>Query builders</h4>
                <a @click="showList=false"><i class="fa fa-times"></i></a>
            </div>
            <div class="panel-body">
                <div class="dd-item" v-for="functionName in queryBuilderFunctions">
                    <div class="left">
                        <span class="list-key"> {{functionName.label}}</span>
                    </div>
                    <div class="right">
                        <button class="btn" @click="$emit('start-builder',functionName.value)">Start</button>
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

.panel-heading {
    padding: 5px 10px;
    display: flex;
    justify-content: space-around;
}

a {
    margin-left: auto;
}

.fa-times{
  cursor: pointer;
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

.dropdown-content {
    position: absolute;
    top: 100%;
    left: -40%;
    z-index: 2;
    margin-top: 5px;
    background-color: #0f0f0f;
}

.right{
  display: inline-flex;
  align-items: center;
}

.left{
    display: inline-flex;
    position: relative;
    border-bottom: 1px solid #606060;
    padding-bottom: 5px;
    flex: 3;
    margin-right: 10px;
}

</style>

<script>
export default {
    data() {
        return {
            showList: false,
            queryBuilderFunctions: [{
                    label: 'Shortest Path',
                    value: 'computePath'
                },
                {
                    label: 'Explore relations',
                    value: 'relationsBetweenNodes'
                }
            ],
        }
    },

    created() {},

    mounted: function() {
        this.$nextTick(function() {
            // code for previous attach() method.
        });
    },

    methods: {}
}
</script>
