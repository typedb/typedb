//Modules
import HALParser from '../js/HAL/HALParser.js';
import {
    URL_REGEX
} from '../js/HAL/HALParser.js';
import * as API from '../js/HAL/APITerms';

// External objects
import EngineClient from '../js/EngineClient.js';
import Visualiser from '../js/visualiser/Visualiser.js';
import User from '../js/User.js'


// Components
var GraqlEditor = require('./graqlEditor.vue')

export default {
    name: "VisualiserView",
    components: {
        GraqlEditor
    },
    data: function() {
        return {
            errorMessage: undefined,
            errorPanelClass: undefined,
            visualiser: {},
            halParser: {},
            analyticsStringResponse: undefined,
            typeInstances: false,
            typeKeys: [],
            doubleClickTime: 0,
            useReasoner: User.getReasonerStatus(),
            materialiseReasoner:User.getMaterialiseStatus(),


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

    created: function() {
        window.visualiser = new Visualiser();
        visualiser.setOnDoubleClick(this.doubleClick)
            .setOnRightClick(this.rightClick)
            .setOnClick(this.singleClick)
            .setOnDragEnd(this.dragEnd)
            .setOnHoldOnNode(this.holdOnNode);

        this.halParser = new HALParser();

        this.halParser.setNewResource((id, p, a, l) => visualiser.addNode(id, p, a, l));
        this.halParser.setNewRelationship((f, t, l) => visualiser.addEdge(f, t, l));
        this.halParser.setNodeAlreadyInGraph(id => visualiser.nodeExists(id));
    },

    mounted: function() {
        this.$nextTick(function() {

            var graph = this.$refs.graph;
            visualiser.render(graph);

            function resizeElements() {
                // set graph div height
                var divHeight = window.innerHeight - graph.offsetTop - $('#graph-div').offset().top - 20;
                $('#graph-div').height(divHeight);
                //set the height of right panel of same size of graph-div
                $('.properties-tab').height(divHeight + 7);
                //fix the height of panel-body so that it is possible to make it overflow:scroll
                $('.properties-tab .panel-body').height(divHeight - 85);
            };
            resizeElements();
            window.onresize = resizeElements;

            $('.properties-tab').hide();
            var height = window.innerHeight - graph.offsetTop - $('#graph-div').offset().top + 20;
            // make the list of resources tab resizable with mouse - jQueryUI
            $('#list-resources-tab').resizable({
                //make it fixed height and only resizable towards west
                minHeight: height,
                maxHeight: height,
                handles: "w"
            });
        });
    },

    methods: {

        onLoadOntology() {
            let query_isa = "match $x isa " + API.TYPE_TYPE + ";";
            let query_sub = "match $x sub " + API.TYPE_TYPE + ";";
            EngineClient.graqlHAL(query_sub, this.onGraphResponse, this.useReasoner,this.materialiseReasoner);
            EngineClient.graqlHAL(query_isa, this.onGraphResponse, this.useReasoner,this.materialiseReasoner);
        },

        singleClick(param) {
            let t0 = new Date();
            let threshold = 200;
            //all this fun to be able to distinguish a single click from a double click
            if (t0 - this.doubleClickTime > threshold) {
                setTimeout(() => {
                    if (t0 - this.doubleClickTime > threshold) {
                        this.leftClick(param);
                    }
                }, threshold);
            }
        },

        onClickSubmit(query) {
            this.errorMessage = undefined;

            if (query.includes("aggregate")) {
                this.showWarning("Invalid query: 'aggregate' queries are not allowed from the Graph page. Please use the Console page.");
                return;
            }

            if (query.trim().startsWith("compute")) {
                EngineClient.graqlAnalytics(query, this.onGraphResponseAnalytics);
            } else {
                EngineClient.graqlHAL(query, this.onGraphResponse, this.useReasoner,this.materialiseReasoner);
            }
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
                if (visualiser.nodes._data[node].ontology) {
                    EngineClient.request({
                        url: visualiser.nodes._data[node].ontology,
                        callback: this.onGraphResponse
                    });
                } else {
                    return;
                }
            else {
                var props = visualiser.getNode(node);
                this.allNodeOntologyProps = {
                    id: props.uuid,
                    type: props.type,
                    baseType: props.baseType
                }

                this.allNodeResources = this.prepareResources(props.properties);
                // this.allNodeLinks = this.sortObject(props.links);

                this.numOfResources = Object.keys(this.allNodeResources).length;
                this.numOfLinks = Object.keys(this.allNodeLinks).length;

                this.selectedNodeLabel = visualiser.getNodeLabel(node);

                $('#list-resources-tab').addClass('active');
                this.openPropertiesTab();
            }
        },
        prepareResources(o) {
            if (o == null) return {};

            //Sort object's keys alphabetically and check if the resource contains a URL string
            return Object.keys(o).sort().reduce(
                //r is the accumulator variable, i.e. new object with sorted keys
                //k is the current key
                (r, k) => {
                    this.checkURLString(o[k]);
                    r[k] = o[k];
                    return r;
                }, {});
        },
        checkURLString(resourceObject) {
            resourceObject.href = this.validURL(resourceObject.label);
        },
        validURL(str) {
            var pattern = new RegExp(URL_REGEX, "i");
            return pattern.test(str);
        },
        dragEnd(param) {
            // As multiselect is disabled, there will only ever be one node.
            const node = param.nodes[0];
            visualiser.disablePhysicsOnNode(node);
        },

        doubleClick(param) {
            this.doubleClickTime = new Date();
            const node = param.nodes[0];
            if (node == undefined || visualiser.expandCluster(node))
                return;

            const eventKeys = param.event.srcEvent;

            if (visualiser.getNode(node).baseType === API.GENERATED_RELATION_TYPE)
                visualiser.deleteNode(node);

            if (eventKeys.shiftKey)
                visualiser.clearGraph();

            EngineClient.request({
                url: node,
                callback: this.onGraphResponse
            });
        },
        addResourceNodeWithOwners(id) {
            EngineClient.request({
                url: id,
                callback: this.onGraphResponse
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
        },
        openPropertiesTab() {
            $('.properties-tab.active').addClass('animated slideInRight');
            $('.properties-tab.active').show();
        },
        /*
         * User interaction: visual elements control
         */
        configureNode(p) {
            if (!(this.nodeType in this.selectedProps)) {
                this.selectedProps[this.nodeType] = [];
            }

            if (this.selectedProps[this.nodeType].includes(p))
                this.selectedProps[this.nodeType] = this.selectedProps[this.nodeType].filter(x => x != p);
            else
                this.selectedProps[this.nodeType].push(p);

            visualiser.setDisplayProperties(this.nodeType, this.selectedProps[this.nodeType]);
        },

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
        holdOnNode(param) {
            const node = param.nodes[0];
            if (node == undefined) return;

            this.allNodeProps = visualiser.getAllNodeProperties(node);
            this.nodeType = visualiser.getNodeType(node);
            $('#myModal2').modal('show');
        },

        onGraphResponseAnalytics(resp, err) {
            if (resp != null) {
                if (resp.type === "string") {
                    this.analyticsStringResponse = resp.response;
                } else {
                    this.halParser.parseResponse(resp.response);
                    visualiser.fitGraphToWindow();
                }
            } else {
                this.showError(err);
            }
        },

        onCloseError() {
            this.errorMessage = undefined;
        },

        /*
         * EngineClient callbacks
         */
        onGraphResponse(resp, err) {
            if (resp != undefined) {
                if (!this.halParser.parseResponse(resp))
                    this.showWarning("Sorry, no results found for your query.");
                else
                    visualiser.cluster();
                visualiser.fitGraphToWindow();
            } else {
                this.showError(err);
            }

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
            this.analyticsStringResponse = undefined;
            $('.search-button')
                .removeClass('btn-danger')
                .removeClass('btn-warning')
                .addClass('btn-default');
        },

        onClear() {
            // Reset all interface elements to default.
            this.closeConfigPanel();
            this.resetMsg();

            // And clear the graph
            visualiser.clearGraph();
        }
    }
}
