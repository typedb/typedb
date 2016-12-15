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
<section class="wrapper">
    <side-bar></side-bar>
    <section class="content">
        <div class="container-fluid">
            <graql-editor v-on:click-submit="onClickSubmit" v-on:load-ontology="onLoadOntology" v-on:clear="onClear" v-on:close-error="onCloseError" showVisualise="true" :errorMessage="errorMessage" :errorPanelClass="errorPanelClass"></graql-editor>
            <div class="row" v-show="analyticsStringResponse">
                <div class="col-xs-12">
                    <div class="panel panel-filled analyticsStringPanel">
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
            <!-- MODAL -->
            <div class="modal fade" id="myModal2" tabindex="-1" role="dialog" aria-hidden="true" style="display: none;">
                <div class="modal-dialog modal-sm">
                    <div class="modal-content">
                        <div class="modal-header text-center">
                            <h5 class="modal-title">Node settings &nbsp;<i style="font-size:35px;" class="pe page-header-icon pe-7s-paint-bucket"></i></h5>
                        </div>
                        <div class="modal-body">
                            <div class="properties-list">
                                <p v-if="allNodeProps.length">Select properties to show on nodes of type "{{nodeType}}".
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
            <keyspaces-modal></keyspaces-modal>
            <signup-modal></signup-modal>
        </div>
    </section>
</section>
</template>

<style>

</style>











<script src="../controllers/visualiser.js"></script>
