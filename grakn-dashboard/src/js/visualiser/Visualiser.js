/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */


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
      click: () => {},
      doubleClick: () => {},
      rightClick: () => {},
      hover: () => {},
      dragEnd: () => {},
      hold: () => {},
    };
    this.style = new Style();

        // vis.js network, instantiated on render.
    this.network = {};

        // vis.js default config
    this.networkConfig = {
      autoResize: true,
      nodes: {
        font: {
          size: 15,
          face: 'Geogrotesque-Ultralight',
        },
        shadow: {
          enabled: true,
          size: 10,
          x: 2,
          y: 2,
        },
      },
      edges: {
        hoverWidth: 2,
        selectionWidth: 2,
        arrowStrikethrough: false,
        arrows: {
          to: true,
        },
        smooth: {
          forceDirection: 'none',
        },
      },
      interaction: {
        hover: true,
        multiselect: false,
      },
      layout: {
        improvedLayout: false,
      },
    };

        // Additional properties to show in node label by type.
    this.displayProperties = {};
    this.alreadyFittedToWindow = false;
    this.clusters = [];
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
     * Register callback for when a node dragging is finished.
     */
  setOnDragEnd(fn) {
    this.callbacks.dragEnd = fn;
    return this;
  }

  setOnHoldOnNode(fn) {
    this.callbacks.hold = fn;
    return this;
  }

    /**
     * Start visualisation and render graph.
     * This needs to be called only once, but all callbacks should be configured
     * prior.
     */
  render(container) {
    this.network = new vis.Network(
            container, {
              nodes: this.nodes,
              edges: this.edges,
            },
            this.networkConfig);

    this.network.on('click', this.callbacks.click);
    this.network.on('doubleClick', this.callbacks.doubleClick);
    this.network.on('oncontext', this.callbacks.rightClick);
    this.network.on('hoverNode', this.callbacks.hover);
    this.network.on('dragEnd', this.callbacks.dragEnd);
    this.network.on('hold', this.callbacks.hold);

    this.network.on('stabilized', () => {
      this.setSimulation(false);
    });

    return this;
  }

  // Fit the graph to the window size only on the first ajax call,
  // then leave zoom control to the user
  fitGraphToWindow() {
    if (!this.alreadyFittedToWindow) {
      this.network.fit();
      this.alreadyFittedToWindow = true;
    }
  }
        /**
         * Add a node to the graph. This can be called at any time *after* render().
         */
  addNode(href, bp, ap, ls) {
    if (!this.nodeExists(bp.id)) {
      const colorObj = this.style.getNodeColour(bp.type, bp.baseType);
      const highlightObj = { highlight: Object.assign(colorObj.highlight, { border: colorObj.highlight.background }) };
      const hoverObj = { hover: highlightObj.highlight };
      this.nodes.add({
        id: bp.id,
        href,
        label: this.generateLabel(bp.type, ap, bp.label),
        baseLabel: bp.label,
        type: bp.type,
        baseType: bp.baseType,
        color: Object.assign(colorObj, { border: colorObj.background }, highlightObj, hoverObj),
        font: this.style.getNodeFont(bp.type, bp.baseType),
        shape: this.style.getNodeShape(bp.baseType),
        selected: false,
        ontology: bp.ontology,
        properties: ap,
        links: ls,
      });
    }

    return this;
  }

  disablePhysicsOnNode(id) {
    if (this.nodeExists(id)) {
      this.nodes.update({
        id,
        physics: false,
      });
    }
    return this;
  }

    /**
     * Add edge between two nodes with @label. This can be called at any time *after* render().
     */
  addEdge(fromNode, toNode, label) {
    if (!this.alreadyConnected(fromNode, toNode)) {
      this.edges.add({
        from: fromNode,
        to: toNode,
        label,
        color: this.style.getEdgeColour(),
        font: this.style.getEdgeFont(),
      });
    }
    return this;
  }

    /**
     * Delete a node and its edges
     */
  deleteNode(id) {
    if (this.nodeExists(id)) {
      this.deleteEdges(id);
      this.nodes.remove(id);
    }
    return this;
  }

    /**
     * Removes all nodes and edges from graph
     */
  clearGraph() {
    this.nodes.clear();
    this.edges.clear();
    this.network.setData({
      nodes: this.nodes,
      edges: this.edges,
    });
    return this;
  }

    /**
     * Stop/start physics simulation and all animation in displayed graph.
     */
  setSimulation(state) {
    if (state) { this.network.startSimulation(); } else {
      this.network.stopSimulation();
    }
    return this;
  }

  getNodeType(id) {
    if (id in this.nodes._data) {
      return this.nodes._data[id].type;
    }
    return undefined;
  }

  getNode(id) {
    return this.nodes._data[id];
  }

  getAllNodeProperties(id) {
    if (id in this.nodes._data) {
      return Object.keys(this.nodes._data[id].properties).sort();
    }
    return [];
  }

  getNodeLabel(id) {
    return this.nodes._data[id].label;
  }

  setDisplayProperties(type, properties) {
    if (type in this.displayProperties && properties.length === 0) {
      delete this.displayProperties[type];
    } else { this.displayProperties[type] = properties; }

    this.updateNodeLabels(type);
    return this;
  }

  cluster() {
    this.clusters.forEach((c) => {
      this.network.cluster({
        joinCondition: x => ((x.baseType !== 'resource-type') && (x.type !== 'resource-type') && (x.type === c)),
        clusterNodeProperties: {
          id: `cluster-${c}`,
          label: `cluster of: ${c}`,
          color: this.style.clusterColour(),
          font: this.style.clusterFont(),
        },
        processProperties: (o, n, e) => {
          if (!this.nodeExists(o.id)) this.nodes.add(o);
          return o;
        },
      });
    });

        //     this.predefinedClusters();
    return this;
  }

  expandCluster(id) {
    if (this.network.isCluster(id)) {
      this.network.openCluster(id);
      this.deleteNode(id);
      return true;
    }

    return false;
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
  static matching(a, b, x, y) {
    return ((a === x && b === y) || (a === y && b === x));
  }

    /**
     * Check if two nodes (a,b) are already connected by an edge.
     */
  alreadyConnected(a, b) {
    if (!(a in this.nodes._data && b in this.nodes._data)) {
      return false;
    }

    return _.contains(_.values(this.edges._data)
            .map(x => Visualiser.matching(a, b, x.to, x.from)),
            true);
  }

    /**
     * Delete all edges connected to node id
     */
  deleteEdges(id) {
    this.edges.forEach((x) => {
      if (x.to === id || x.from === id) this.edges.remove(x.id);
    });
  }

  generateLabel(type, properties, label) {
    if (type in this.displayProperties) {
      return this.displayProperties[type].reduce((l, x) => {
        let value = (properties[x] === undefined) ? '' : properties[x].label;
        if (value.length > 40) value = `${value.substring(0, 40)}...`;
        return `${(l.length ? `${l}\n` : l) + x}: ${value}`;
      }, '');
    }
    return label;
  }

  updateNodeLabels(type) {
    this.nodes._data = _.mapObject(this.nodes._data, (v, k) => {
      if (v.type === type) {
        this.nodes.update({
          id: k,
          label: this.generateLabel(type, v.properties, v.baseLabel),
        });
      }
      return v;
    });

        //   this.cluster();
  }

  addCluster(clusterBy) {
    if (!_.contains(this.clusters, clusterBy)) {
      this.clusters.push(clusterBy);
    }
  }

  predefinedClusters() {
    this.network.cluster({
      joinCondition: x => (x.baseType === 'resource-type'),
      clusterNodeProperties: {
        id: 'cluster-resource-type',
        label: 'cluster of: resource-type',
        color: this.style.clusterColour(),
        font: this.style.clusterFont(),
      },
    });
  }
}
