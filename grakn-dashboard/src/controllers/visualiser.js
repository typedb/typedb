import _ from 'underscore';
import Prism from 'prismjs';
import CodeMirror from 'codemirror';
import placeholder from 'codemirror/addon/display/placeholder.js';
import simpleMode from 'codemirror/addon/mode/simple.js';

import Visualiser from '../js/visualiser/Visualiser.js';
import HALParser from '../js/HAL/HALParser.js';
import EngineClient from '../js/EngineClient.js';
import * as PLang from '../js/prismGraql.js';
import simpleGraql from '../js/codemirrorGraql.js';

export default {
    data() {
            return {
                errorMessage: undefined,
                errorPanelClass: undefined,
                visualiser: {},
                engineClient: {},
                halParser: {},

                typeInstances: false,
                typeKeys: [],

                // resources keys used to change label of a node type
                allNodeProps: [],
                selectedProps: [],
                nodeType: undefined,
                selectedNodeLabel: undefined,
                // resources attached to the selected node
                allNodeOntologyProps: {},
                allNodeResources: {},
                allNodeLinks: {},

                numOfResources: 0,
                numOfLinks: 0,
                codeMirror: {}
            }
        },

        created() {
            visualiser = new Visualiser();
            visualiser.setOnDoubleClick(this.doubleClick)
                .setOnRightClick(this.rightClick)
                .setOnClick(this.leftClick)
                .setOnDragEnd(this.dragEnd)
                .setOnHoldOnNode(this.holdOnNode);

            engineClient = new EngineClient();

            halParser = new HALParser();
            halParser.setNewResource((id, p, a, l) => {
                visualiser.addNode(id, p, a, l)
            });
            halParser.setNewRelationship((f, t, l) => {
                visualiser.addEdge(f, t, l)
            });
        },

        attached() {
            var graph = this.$els.graph;

            function resizeElements() {
                // set graph div height
                var divHeight = window.innerHeight - graph.offsetTop - $('.graph-div').offset().top - 20;
                $('.graph-div').height(divHeight);
                //set the height of right panel of same size of graph-div
                $('.properties-tab').height(divHeight+7);
                //fix the height of panel-body so that it is possible to make it overflow:scroll
                $('.properties-tab .panel-body').height(divHeight - 85);
            };

            visualiser.render(graph);

            codeMirror = CodeMirror.fromTextArea(this.$els.graqlEditor, {
                lineNumbers: true,
                theme: "dracula",
                mode: "graql",
                viewportMargin: Infinity,
                extraKeys: {
                    Enter: this.runQuery,
                    "Shift-Delete": this.clearGraph,
                    "Shift-Backspace": this.clearGraph
                }
            });

            resizeElements();
            window.onresize = resizeElements;

            $('.properties-tab').hide();
            var height = window.innerHeight - graph.offsetTop - $('.graph-div').offset().top + 20;
            // make the list of resources tab resizable with mouse - jQueryUI
            $('#list-resources-tab').resizable({
                //make it fixed height and only resizable towards west
                minHeight: height,
                maxHeight: height,
                handles: "w"
            });

        },

        methods: {
            /*
             * User interaction: queries.
             */
            runQuery() {
                const query = codeMirror.getValue();

                // Empty query.
                if (query == undefined || query.length === 0)
                    return;

                engineClient.graqlHAL(query, this.graphResponse);
                this.resetMsg();
            },

            typeQuery(t, ti) {
                codeMirror.setValue("match $x " + (t === 'roles' ? 'plays-role' : 'isa') + " " + ti + ";");
                this.typeInstances = false;
                this.runQuery();
            },

            getMetaTypes() {
                if (this.typeInstances)
                    this.typeInstances = false;
                else
                    engineClient.getMetaTypes(x => {
                        if (x != null) {
                            this.typeInstances = x;
                            this.typeKeys = _.keys(x)
                        }
                    });
            },

            /*
             * User interaction: visualiser
             */
            leftClick(param) {
                // As multiselect is disabled, there will only ever be one node.
                const node = param.nodes[0];
                const eventKeys = param.event.srcEvent;
                const clickType = param.event.type;

                if (node == undefined || eventKeys.shiftKey || clickType !== "tap")
                    return;

                //When we will enable clustering, also need to check && !visualiser.expandCluster(node)
                if (eventKeys.altKey)
                    engineClient.request({
                        url: visualiser.nodes._data[node].ontology,
                        callback: this.typeQueryResponse
                    });
                else {
                    var props = visualiser.getNode(node);
                    this.allNodeOntologyProps = {
                        id: props.uuid,
                        type: props.type,
                        baseType: props.baseType
                    }

                    this.allNodeResources = (props.properties != null) ? this.sortObject(props.properties) : {};
                    this.allNodeLinks = this.sortObject(props.links);

                    this.numOfResources = Object.keys(this.allNodeResources).length;
                    this.numOfLinks = Object.keys(this.allNodeLinks).length;

                    this.selectedNodeLabel = visualiser.getNodeLabel(node);

                    $('#list-resources-tab').addClass('active');
                    this.openPropertiesTab();
                }
            },
            sortObject(o) {
                return Object.keys(o).sort().reduce((r, k) => (r[k] = o[k], r), {});
            },
            dragEnd(param) {
                // As multiselect is disabled, there will only ever be one node.
                const node = param.nodes[0];
                visualiser.disablePhysicsOnNode(node);
            },

            doubleClick(param) {
                const node = param.nodes[0];
                if (node == undefined || visualiser.expandCluster(node))
                    return;

                const eventKeys = param.event.srcEvent;
                if (!eventKeys.shiftKey)
                    visualiser.clearGraph();

                engineClient.request({
                    url: node,
                    callback: this.typeQueryResponse
                });
            },
            addResourceNodeWithOwners(id){
              engineClient.request({
                  url: id,
                  callback: this.typeQueryResponse
              });
            },
            rightClick(param) {
                const node = param.nodes[0];
                if (node == undefined)
                    return;

                if (param.event.shiftKey) {
                    param.nodes.map(x => {
                        visualiser.deleteNode(x)
                    });
                }
                // else if (!visualiser.expandCluster(node)) {
                //     this.allNodeProps = visualiser.getAllNodeProperties(node);
                //     this.nodeType = visualiser.getNodeType(node);
                //     $('#label-tab').addClass("active");
                //     this.openPropertiesTab();
                // }
            },
            openPropertiesTab() {
                $('.properties-tab.active').addClass('animated slideInRight');
                $('.properties-tab.active').show();
            },
            /*
             * User interaction: visual elements control
             */
            configureNode(p) {
                if (this.selectedProps.includes(p))
                    this.selectedProps = this.selectedProps.filter(x => x != p);
                else
                    this.selectedProps.push(p);

                console.log("ehi proprieta' "+JSON.stringify(this.selectedProps));

                visualiser.setDisplayProperties(this.nodeType, this.selectedProps);
            },

            closeConfigPanel() {
                if ($('.properties-tab.active').hasClass('slideInRight')) {
                    $('.properties-tab.active').removeClass('animated slideInRight');
                    $('.properties-tab.active').fadeOut(300, function() {
                        this.nodeType = undefined;
                        this.allNodeProps = [];
                        this.selectedProps = [];
                    });
                    $('.properties-tab.active').removeClass('active');
                }
            },
            holdOnNode(param) {
                const node = param.nodes[0];
                if (node == undefined) return;

                this.allNodeProps = visualiser.getAllNodeProperties(node);
                this.nodeType = visualiser.getNodeType(node);
                //$('#label-tab').addClass("active");
                $('#myModal2').modal('show');

                // this.openPropertiesTab();
            },
            /*
             * EngineClient callbacks
             */
            graphResponse(resp, err) {
                if (resp != null) {
                    if (!halParser.parseResponse(resp))
                        this.showWarning("Sorry, no results found for your query.");
                    else
                        visualiser.cluster();
                } else {
                    this.showError(err);
                }
            },

            typeQueryResponse(resp, err) {
                if (resp != undefined) {
                    halParser.parseHalObject(resp);
                    visualiser.cluster();
                } else {
                    this.showError(err);
                }
            },

            /*
             * UX
             */
            suppressEventDefault(e) {
                e.preventDefault();
            },

            showError(msg) {
                this.errorPanelClass = 'panel-c-danger';
                this.errorMessage = msg;
                $('.search-button').removeClass('btn-default').addClass('btn-danger');
            },

            showWarning(msg) {
                this.errorPanelClass = 'panel-c-warning';
                this.errorMessage = msg;
                $('.search-button').removeClass('btn-default').addClass('btn-warning');
            },

            resetMsg() {
                this.errorMessage = undefined;
                $('.search-button')
                    .removeClass('btn-danger')
                    .removeClass('btn-warning')
                    .addClass('btn-default');
                this.closeConfigPanel();
            },

            clearGraph() {
                // Reset all interface elements to default.
                codeMirror.setValue("");
                this.resetMsg();

                // And clear the graph
                visualiser.clearGraph();
            }
        }
}
