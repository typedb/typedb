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
<transition name="slideInUp" appear>
    <div class="footer-bar  z-depth-1-half">
        <div class="left"><node-label-panel></node-label-panel></diV>
        <div class="center"><analytics-panel></analytics-panel></diV>
        <div class="right"></diV>
    </div>
</transition>
</template>

<style scoped>
.footer-bar {
    z-index: 2;
    position: absolute;
    background-color: #17191d;
    bottom: 0px;
    display: flex;
    width: 100%;
    min-height: 30px;
}

.left{
  display: inline-flex;
  flex:1;
  position: relative;
}
.center{
  display: inline-flex;
  flex: 3;
  position: relative;
}
.right{
  display: inline-flex;
  flex: 1;
}
</style>

<script>
const NodeLabelPanel = require('./nodeLabelPanel.vue');
const AnalyticsPanel = require('./analyticsResultsContainer.vue');

export default {
    name: "FooterBar",
    components:{
      NodeLabelPanel,
      AnalyticsPanel
    },
    data: function() {
        return {}
    },
    created: function() {},
    mounted: function() {
        this.$nextTick(function() {

        })
    },
    methods: {


    }
}
</script>
