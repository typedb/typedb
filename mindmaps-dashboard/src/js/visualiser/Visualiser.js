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
            edges: { arrows: { to: true } },
            physics: { solver: "forceAtlas2Based" },
            interaction: {
                hover: true,
                multiselect: false
            }
        };
    }

    /**
     * Register callback for mouse click on nodes or edges
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

        return this;
    }

    /**
     * Add a node to the graph. This can be called at any time *after* render().
     */
    addNode(id, label, type, baseType) {
        if(!this.nodeExists(id))
            this.nodes.add({
                id: id,
                label: label,
                type: type,
                color: this.style.getNodeColour(baseType),
                font: this.style.getNodeFont(baseType),
                shape: this.style.getNodeShape(baseType),
                selected: false
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
}
