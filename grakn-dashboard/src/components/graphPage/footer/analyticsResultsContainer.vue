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
<transition name="slideInUp">
    <div class="panel-wrapper  z-depth-1-half" v-show="showAnalyticsPanel">
        <div class="modal-header text-center">
            <h5 class="modal-title">ANALYTICS RESULT</h5>
        </div>
        <div class="panel-body">
            {{analyticsResponse}}
        </div>
        <div class="panel-footer">
            <button class="btn" @click="showAnalyticsPanel=false;">Done</button>
        </div>
    </div>
</transition>
</template>

<style scoped>

.panel-wrapper{
  position: absolute;
  bottom: 100%;
  background-color: #0f0f0f;
  width:100%;
  margin-bottom:5px;
  padding: 10px 10px;
}

.panel-footer{
  display: flex;
  justify-content: flex-end;
}

.dd-item{
  cursor: pointer;
  padding: 5px;
  color: white;
  border: 1px solid white;
  margin: 2px 0px;
}

.modal-title{
  margin-bottom: 10px;
}

h5{
  border-bottom: 1px solid #606060;
  padding-bottom:5px;
}

</style>

<script>

import GraphPageState from '../../../js/state/graphPageState';


export default {
    name: 'AnalyticsResultsPanel',
    data() {
        return {
            state:GraphPageState,
            showAnalyticsPanel:false,
            analyticsResponse:''
        };
    },
    created() {
        this.state.eventHub.$on('analytics-string-response',this.addAnalyticsResponse);
    },
    mounted() {
        this.$nextTick(function nextTickVisualiser() {

        });
    },
    methods: {
      addAnalyticsResponse(response){
          this.analyticsResponse=response;
          this.showAnalyticsPanel=true;
      }
    }
};
</script>
