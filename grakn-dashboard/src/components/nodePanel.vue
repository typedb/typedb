<template>
<div class="panel panel-filled panel-c-accent properties-tab" id="list-resources-tab">
    <div class="panel-heading">
        <div class="panel-tools">
            <a class="panel-close" @click="closeNodePanel"><i class="fa fa-times"></i></a>
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
            <span v-show="Object.keys(allNodeResources).length">Resources:</span>
            <div class="dd-item" v-for="(value,key) in allNodeResources">
                <div class="dd-handle" @dblclick="addResourceNodeWithOwners(value.link)"><span class="list-key">{{key}}:</span>
                    <a v-if="value.href" :href="value.label" style="word-break: break-all;" target="_blank">{{value.label}}</a>
                    <span v-else> {{value.label}}</span>
                </div>
            </div>
            <span v-show="Object.keys(allNodeLinks).length">Links:</span>
            <div class="dd-item" v-for="(value, key) in allNodeLinks">
                <div class="dd-handle"><span class="list-key">{{key}}:</span> {{value}}</div>
            </div>
        </div>
    </div>
</div>
</template>

<style>
</style>

<script>
import EngineClient from '../js/EngineClient';
import User from '../js/User';

export default {
    name: "NodePanel",
    props: ['allNodeResources', 'allNodeLinks', 'allNodeOntologyProps', 'selectedNodeLabel'],
    data: function() {
        return {
        }
    },
    created: function() {},
    mounted: function() {
        this.$nextTick(function() {})
    },
    methods: {
      closeNodePanel() {
        this.$emit('close-node-panel');
      },
      addResourceNodeWithOwners(id) {
        EngineClient.request({
          url: id,
          callback: (resp)=>this.$emit('graph-response',resp),
        });
      }
    }
}
</script>
