/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

"use strict";

import _ from 'underscore';
import vis from 'vis';

import Style from './Style';

/*
 * Main class for creating a graph of nodes and edges. See Style class for asthetic customisation.
 * Callbacks (for interactivity with the graph) must be registered before calling .render().
 * Graph is drawn *only* after calling .render().
 * Nodes and edges can be added at any time.
 */
export default class Visualiser {
    constructor() {
        this.nodes = new vis.DataSet([]);
        this.edges = new vis.DataSet([]);

        this.callbacks = {
            click: x => {},
            doubleClick: x => {},
            rightClick: x => {},
            hover: x => {}
        };

        this.style = new Style();

        // vis.js network, instantiated on render.
        this.network = {};

        // vis.js default config
        this.networkConfig = {
            autoResize: true,
            edges: {
                arrows: { to: true },
                smooth: { forceDirection: 'none' }
            },
            physics: {
                "repulsion": {
                  "centralGravity": 0.6,
                  "springLength": 360,
                  "springConstant": 0.09,
                  "nodeDistance": 350,
                  "damping": 0.82
                },
                "minVelocity": 0.75,
                "solver": "repulsion"
            },
            interaction: {
                hover: true,
                multiselect: false
            }
        };

        // Additional properties to show in node label by type.
        this.displayProperties = {};
    }

    /**
     * Register callback for mouse click on nodes or edges.
     */
    setOnClick(fn) {
        this.callbacks.click = fn;
        return this;
    }

    /**
     * Register callback for double click on nodes or edges.
     */
    setOnDoubleClick(fn) {
        this.callbacks.doubleClick = fn;
        return this;
    }

    /**
     * Register callback for right click on node or edges.
     */
    setOnRightClick(fn) {
        this.callbacks.rightClick = fn;
        return this;
    }

    /**
     * Register callback for mouse hover on nodes.
     */
    setOnHover(fn) {
        this.callbacks.hover = fn;
        return this;
    }

    /**
     * Start visualisation and render graph. This needs to be called only once, but all callbacks should be configured
     * prior.
     */
    render(container) {
        this.network = new vis.Network(
            container,
            { nodes: this.nodes, edges: this.edges },
            this.networkConfig);

        this.network.on('click', this.callbacks.click);
        this.network.on('doubleClick', this.callbacks.doubleClick);
        this.network.on('oncontext', this.callbacks.rightClick);
        this.network.on('hoverNode', this.callbacks.hover);
        this.network.on('stabilized', () => { this.setSimulation(false) });

        return this;
    }

    /**
     * Add a node to the graph. This can be called at any time *after* render().
     */
    addNode(id, bp, ap) {
        if(!this.nodeExists(id))
            this.nodes.add({
                id: id,
                label: this.generateLabel(bp.type, ap, bp.label),
                baseLabel: bp.label,
                type: bp.type,
                color: this.style.getNodeColour(bp.baseType),
                font: this.style.getNodeFont(bp.baseType),
                shape: this.style.getNodeShape(bp.baseType),
                selected: false,
                ontology: bp.ontology,
                properties: ap
            });

        return this;
    }

    /**
     * Add edge between two nodes with @label. This can be called at any time *after* render().
     */
    addEdge(fromNode, toNode, label) {
        if(!this.alreadyConnected(fromNode, toNode))
            this.edges.add({
                from: fromNode,
                to: toNode,
                label: label,
                color: this.style.getEdgeColour(),
                font: this.style.getEdgeFont()
            });

        return this;
    }

    /**
     * Delete a node and its edges
     */
    deleteNode(id) {
        if(this.nodeExists(id)) {
            this.deleteEdges(id);
            this.nodes.remove(id);
        }
    }

    /**
     * Removes all nodes and edges from graph
     */
    clearGraph() {
        this.nodes.clear();
        this.edges.clear();
    }

    /**
     * Stop/start physics simulation and all animation in displayed graph.
     */
    setSimulation(state) {
        if(state)
            this.network.startSimulation();
        else
            this.network.stopSimulation();
        return this;
    }

    getNodeType(id) {
        if(id in this.nodes._data)
            return this.nodes._data[id].type;
        return undefined;
    }

    getAllNodeProperties(id) {
        if(id in this.nodes._data)
            return Object.keys(this.nodes._data[id].properties);

        return []
    }

    setDisplayProperties(type, properties) {
        this.displayProperties[type] = properties;
        this.updateNodeLabels(type);
    }

    /*
    Internal methods
    */

    /**
     * Check if node has already been added to graph.
     */
    nodeExists(id) {
        return (id in this.nodes._data);
    }

    /**
     * Check if (a,b) match (x,y) in either combination.
     */
    matching(a, b, x, y) {
        return ((a === x && b === y) || (a === y && b === x));
    }

    /**
     * Check if two nodes (a,b) are already connected by an edge.
     */
    alreadyConnected(a, b) {
        if(!(a in this.nodes._data && b in this.nodes._data))
            return false;

        return _.contains(_.values(this.edges._data)
            .map(x => { return this.matching(a, b, x.to, x.from) }),
            true);
    }

    /**
     * Delete all edges connected to node nid
     */
    deleteEdges(nid) {
        this.edges.map(x => { if(x.to === nid || x.from === nid) this.edges.remove(x.id) });
    }

    generateLabel(type, properties, label) {
        if(type in this.displayProperties)
            return this.displayProperties[type].reduce((l, x) => { return l + "\n"+x+": "+properties[x] }, "");
        else
            return label;
    }

    updateNodeLabels(type) {
        this.nodes._data = _.mapObject(this.nodes._data,
            (v, k) => { if(v.type === type) v.label = this.generateLabel(type, v.properties, v.baseLabel); return v; });

        this.network.setData({nodes: this.nodes, edges: this.edges});
    }
}
