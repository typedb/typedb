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
<div class="container-fluid">
    <div class="row">
        <div class="col-xs-12">
            <div class="panel panel-filled" id="panel-console-container">
                <div class="panel-body row" id="panel-console">
                    <div class="form-group col-xs-8" style="margin-bottom:0px;">
                        <textarea v-el:graql-editor class="form-control" rows="3" placeholder=">>"></textarea>
                    </div>
                    <div class="form-buttons col-xs-4">
                        <button @click="getMetaTypes" class="btn btn-info console-button">Types<i class="types-button"
                                                                                          v-bind:class="[typeInstances ? 'pe-7s-angle-up-circle' : 'pe-7s-angle-down-circle']"></i>
                          </button>
                        <button @click="clearGraph" class="btn btn-default console-button">Clear<i class="pe-7s-refresh"></i>
                          </button>
                        <button @click="runQuery" class="btn btn-default search-button console-button">Submit<i
                                    class="pe-7s-angle-right-circle"></i></button>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <div v-show="typeInstances">
        <div class="panel panel-filled" style="margin-bottom: 0px; margin-top: 20px;">
            <div class="tabs-col">
                <div class="row">
                    <div class="col-xs-10">
                        <div class="tabs-container">
                            <ul class="nav nav-tabs">
                                <li v-for="k in typeKeys"><a data-toggle="tab" href="#{{k}}-tab" aria-expanded="false">{{k
                                    | capitalize}}</a></li>
                            </ul>
                        </div>
                    </div>
                    <div class="col-xs-2">
                      <button @click="loadOntology" class="btn btn-default console-button" id="ontology-button">Visualise</button>
                    </div>
                </div>
                <div class="tab-content">
                    <div v-for="k in typeKeys" id="{{k}}-tab" class="tab-pane">
                        <div class="panel-body types-panel" style="margin: 0px;">
                            <div class="{{k}}-group row m-t-md" style="margin-top: 0px;">
                                <div class="col-lg-2 col-md-3 col-sm-6 col-xs-6 type-instance" v-for="i in typeInstances[k]">
                                    <button @click="typeQuery(k, i)" class="btn btn-link">{{i}}</button>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <div class="row" v-show="errorMessage">
        <div class="col-xs-12">
            <div class="panel panel-filled" v-bind:class="errorPanelClass">
                <div class="panel-body">
                    {{errorMessage}} <a href="#" @click="resetMsg"><i class="pe-7s-close-circle grakn-icon"></i></a>
                </div>
            </div>
        </div>
    </div>

    <div class="row" v-show="analyticsStringResponse">
        <div class="col-xs-12">
            <div class="panel panel-filled" class="analyticsStringPanel">
              <div class="panel-heading">Analytics Results</div>
                <div class="panel-body">
                    <pre class="language-graql">{{analyticsStringResponse}}</pre>
                </div>
            </div>
        </div>
    </div>

    <div class="row tab-row">
        <div class="tabs-col col-md-12">
            <div class="tabs-container">
                <ul class="nav nav-tabs">
                    <li class="active"><a data-toggle="tab" href="#tab-1" aria-expanded="true">Graph</a></li>
                    <li class=""><a data-toggle="tab" href="#tab-3" aria-expanded="false">Help</a></li>
                </ul>
                <div class="tab-content">
                    <div id="tab-1" class="tab-pane active">
                        <div class="panel-body graph-panel-body">
                            <div class="graph-div" v-el:graph @contextmenu="suppressEventDefault"></div>
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
                                            <div class="dd-item" v-for="(key, value) in allNodeOntologyProps">
                                                <div><span class="list-key">{{key}}:</span> {{value}}</div>
                                            </div>
                                        </div>
                                        <span v-show="numOfResources>0">Resources:</span>
                                        <div class="dd-item" v-for="(key, value) in allNodeResources">
                                            <div class="dd-handle" @dblclick="addResourceNodeWithOwners(value.link)"><span class="list-key">{{key}}:</span>
                                              <a v-if="value.href" href="{{value.label}}" style="word-break: break-all;" target="_blank">{{value.label}}</a>
                                              <span v-else> {{value.label}}</span>
                                            </div>
                                        </div>
                                        <span v-show="numOfLinks>0">Links:</span>
                                        <div class="dd-item" v-for="(key, value) in allNodeLinks">
                                            <div class="dd-handle"><span class="list-key">{{key}}:</span> {{value}}</div>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                    <div id="tab-3" class="tab-pane">
                        <div class="panel-body">
                            <h4>Graql Entry</h4>
                            <br/>
                            <div class="table-responsive">
                                <table class="table table-hover table-striped">
                                    <thead>
                                        <tr>
                                            <th>Key</th>
                                            <th>What it does</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        <tr>
                                            <td>ENTER</td>
                                            <td>Submit Graql query.</td>
                                        </tr>
                                        <tr>
                                            <td>Shift + Enter</td>
                                            <td>New line.</td>
                                        </tr>
                                        <tr>
                                            <td>Shift + Backspace</td>
                                            <td>Clear graph & current query.</td>
                                        </tr>
                                        <tr>
                                            <td>Shift + Delete</td>
                                            <td>Clear graph & current query.</td>
                                        </tr>
                                    </tbody>
                                </table>
                            </div>
                            <br/>
                            <br/>
                            <h4>Graph Tab Interaction</h4>
                            <br/>
                            <div class="table-responsive">
                                <table class="table table-hover table-striped">
                                    <thead>
                                        <tr>
                                            <th>Action</th>
                                            <th>What it does</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        <tr>
                                            <td>Left Click</td>
                                            <td>Selects a node or edge.</td>
                                        </tr>
                                        <tr>
                                            <td>Left Click + Alt</td>
                                            <td>Show related ontology of selected node(s).</td>
                                        </tr>
                                        <tr>
                                            <td>Double Click</td>
                                            <td>Shows instances and isa of selected node(s), <b>WITHOUT</b> clearing the graph of all other non-related nodes.
                                            </td>
                                        </tr>
                                        <tr>
                                            <td>Double Click + Shift</td>
                                            <td>Shows instances and isa of selected node(s), whilst clearing the graph of all other non-related nodes.
                                            </td>
                                        </tr>
                                        <tr>
                                            <td>Hold Click</td>
                                            <td>Show node label configuration menu. You can select what properties to display on the node label.
                                            </td>
                                        </tr>
                                        <tr>
                                            <td>Right Click + Shift</td>
                                            <td>Delete selected node(s).</td>
                                        </tr>
                                        <tr>
                                            <td>Scroll wheel</td>
                                            <td>Zoom in/out.</td>
                                        </tr>
                                        <tr>
                                            <td>Click & Drag</td>
                                            <td>Move graph.</td>
                                        </tr>
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
</div>
<!-- MODAL -->
<div class="modal fade" id="myModal2" tabindex="-1" role="dialog" aria-hidden="true" style="display: none;">
    <div class="modal-dialog modal-sm">
        <div class="modal-content">
            <div class="modal-header text-center">
                <h5 class="modal-title">Node settings &nbsp;<i style="font-size:35px;" class="pe page-header-icon pe-7s-paint-bucket"></i></h5>
            </div>
            <div class="modal-body">
                <div class="properties-list">
                    <p v-show="allNodeProps.length">Select properties to show on nodes of type "{{nodeType}}".
                    </p>
                    <p v-else>There is nothing configurable for nodes of type "{{nodeType}}".</p>
                    <br/>
                    <ul class="dd-list">
                        <li class="dd-item" v-for="prop in allNodeProps" v-bind:class="{'li-active':selectedProps.includes(prop)}">
                            <div class="dd-handle" @click="configureNode(prop)">{{prop}}</div>
                        </li>
                    </ul>
                </div>
            </div>
            <div class="modal-footer">
                <button type="button" class="btn btn-default" data-dismiss="modal">Done</button>
            </div>
        </div>
    </div>
</div>
</template>

<style>

</style>



<script src="../controllers/visualiser.js"></script>
