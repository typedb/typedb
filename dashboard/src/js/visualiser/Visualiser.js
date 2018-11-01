/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

import _ from 'underscore';
import vis from 'vis';

import Style from './Style';
import User from '../User';
import NodeSettings from '../NodeSettings';
import * as API from '../Parser/APIUtils';


/*
 * Main class for creating a graph of nodes and edges. See Style class for asthetic customisation.
 * Callbacks (for interactivity with the graph) must be registered before calling .render().
 * Graph is drawn *only* after calling .render().
 * Nodes and edges can be added at any time.
 */
export default class Visualiser {
  constructor(graphOffsetTop) {
    this.graphOffsetTop = graphOffsetTop;
    this.nodes = new vis.DataSet([]);
    this.edges = new vis.DataSet([]);

    this.callbacks = {};
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
      physics: {
        barnesHut: {
          springLength: 140,
        },
        minVelocity: 0.75,
      },
      edges: {
        hoverWidth: 2,
        selectionWidth: 2,
        arrowStrikethrough: false,
        arrows: { to: { enabled: true, scaleFactor: 0.7 } },
        smooth: {
          enabled: false,
          forceDirection: 'none',
        },
      },
      interaction: {
        hover: true,
        multiselect: true,
      },
      layout: {
        improvedLayout: false,
        randomSeed: 10,
      },
    };

    // Additional properties to show in node label by type.
    this.displayProperties = {};
    this.alreadyFittedToWindow = false;
    // Structure to hold colour preferences on nodes
    this.nodeColourProperies = {};

    // working on stopping nodes from moving
    this.lastFixTime = 0; // this is needed to stop a redraw loop due to the update of the vis dataset
    this.draggingNode = false;

    this.draggingRect = false;
    this.rectangleCleared = false;
  }

  setCallbackOnEvent(eventName, callback) {
    this.callbacks[eventName] = callback;
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
      this.networkConfig,
    );

    for (const eventName in this.callbacks) {
      this.network.on(eventName, this.callbacks[eventName]);
    }

    this.network.on('stabilized', (params) => {
      if (this.draggingNode === false && User.getFreezeNodes()) {
        this.fixNodes();
      }
    });

    this.network.on('dragEnd', (params) => {
      this.draggingNode = false;
      this.fixNodes(params.nodes);
    });


    this.network.on('afterDrawing', (params) => {
      if (this.draggingRect) {
        this.drawRectangle();
      }
    });

    // Variables used to draw selection rectangle.
    this.canvas = this.network.canvas.frame.canvas;
    this.ctx = this.canvas.getContext('2d');
    this.rect = {};


    return this;
  }

  // Methods used to draw a selection rectangle on the canvas and select multiple nodes
  resetRectangle() {
    this.draggingRect = false;
    this.selectNodesFromHighlight();
    this.rect.w = 0;
    this.rect.h = 0;
  }

  startRectangle(x, y) {
    this.rect.startX = x;
    this.rect.startY = y;
  }

  updateRectangle(x, y) {
    if (this.draggingRect) {
      const canvasPosition = this.network.DOMtoCanvas({
        x,
        y,
      });
      this.rect.w = canvasPosition.x - this.rect.startX;
      this.rect.h = canvasPosition.y - this.rect.startY;
      // Force redraw the canvas which will also draw the rectangle with new size.
      this.network.redraw();
    }
  }

  drawRectangle() {
    this.ctx.setLineDash([5]);
    this.ctx.strokeStyle = 'rgb(0, 102, 0)';
    this.ctx.strokeRect(this.rect.startX, this.rect.startY, this.rect.w, this.rect.h);
    this.ctx.setLineDash([]);
    this.ctx.fillStyle = 'rgba(0, 255, 0, 0.2)';
    this.ctx.fillRect(this.rect.startX, this.rect.startY, this.rect.w, this.rect.h);
  }

  selectNodesFromHighlight() {
    const nodesIdInDrawing = [];
    const xRange = Visualiser.getStartToEnd(this.rect.startX, this.rect.w);
    const yRange = Visualiser.getStartToEnd(this.rect.startY, this.rect.h);

    const allNodes = this.nodes.get();
    const arrayLength = allNodes.length;
    for (let i = 0; i < arrayLength; i++) {
      const curNode = allNodes[i];
      const nodePosition = this.network.getPositions([curNode.id]);
      const nodeXY = nodePosition[curNode.id];
      if (xRange.start <= nodeXY.x && nodeXY.x <= xRange.end && yRange.start <= nodeXY.y && nodeXY.y <= yRange.end) {
        nodesIdInDrawing.push(curNode.id);
      }
    }
    this.network.selectNodes(nodesIdInDrawing);
  }

  static getStartToEnd(start, theLen) {
    return theLen > 0 ? {
      start,
      end: start + theLen,
    } : {
      start: start + theLen,
      end: start,
    };
  }

  //  ----------------------------------------------  //

  fixAllNodes() {
    this.fixNodes(this.nodes.getIds());
  }

  releaseAllNodes() {
    this.releaseNodes(this.nodes.getIds());
  }

  // Methods used to fix and release nodes when one or more are dragged /

  fixNodes(nodeIds) {
    if (new Date() - this.lastFixTime > 100) {
      this.lastFixTime = new Date();
      if (nodeIds !== undefined) {
        nodeIds.forEach(nodeId => this.fixSingleNode(nodeId));
      } else {
        this.nodes.forEach((node) => {
          this.fixSingleNode(node.id);
        });
      }
    }
  }

  fixSingleNode(nodeId) {
    if (nodeId === undefined) return;
    this.updateNode({
      id: nodeId,
      fixed: {
        x: true,
        y: true,
      },
    });
  }

  releaseNodes(nodeIds) {
    if (nodeIds === undefined) return;
    nodeIds.forEach(nodeId => this.updateNode({
      id: nodeId,
      fixed: {
        x: false,
        y: false,
      },
    }));
  }

  // --------------------------  //


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
  addNode(node) {
    if (this.nodeExists(node.id)) return this;
    const colorObj = this.style.getNodeColour(node.type, node.baseType);
    const highlightObj = {
      highlight: Object.assign(colorObj.highlight, {
        border: colorObj.highlight.background,
      }),
    };
    const hoverObj = {
      hover: highlightObj.highlight,
    };
    this.nodes.add(Object.assign(node, {
      label: node.label,
      baseLabel: node.label,
      color: Object.assign(colorObj, {
        border: colorObj.background,
      }, highlightObj, hoverObj),
      font: this.style.getNodeFont(node.type, node.baseType),
      shape: this.style.getNodeShape(node.baseType),
      size: this.style.getNodeSize(node.baseType),
    }));
    return this;
  }

  updateNodeAttributes(id, properties) {
    this.updateNode({
      id,
      properties,
    });
  }
  /**
     * Add edge between two nodes with @label, only if both nodes exist in the graph and they are not alreay connected.
     * This can be called at any time *after* render().
     */
  addEdge(edge) {
    if (this.nodeExists(edge.from) && this.nodeExists(edge.to) && !this.alreadyConnected(edge)) {
      this.edges.add({
        from: edge.from,
        to: edge.to,
        label: edge.label,
        color: this.style.getEdgeColour(edge.label),
        font: this.style.getEdgeFont(edge.label),
        arrows: {
          to: true,
        },
      });
      const connectingEdge = this.edgesBetweenTwoNodes(edge.from, edge.to);
      // If there are multiple edges connecting the same 2 nodes make the edges smooth so that the labels are visible
      if (connectingEdge.length > 1) {
        connectingEdge.forEach((edgeId) => {
          this.edges.update({ id: edgeId, smooth: { enabled: true, type: 'dynamic' } });
        });
      }
    }
    return this;
  }

  /**
     * Delete a node and its edges
     */
  deleteNode(id:string) {
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

  getNode(id) {
    return this.nodes.get(id);
  }

  getNodeLabel(id) {
    return this.nodes._data[id].label;
  }

  setDisplayProperties(type, properties) {
    if (type in this.displayProperties && properties.length === 0) {
      delete this.displayProperties[type];
    } else {
      this.displayProperties[type] = properties;
    }

    this.updateNodeLabels(type);
    return this;
  }

  setDefaultNodeColour(baseType, nodeType) {
    const colorObj = this.style.getDefaultNodeColour(nodeType, baseType);
    const highlightObj = {
      highlight: Object.assign(colorObj.highlight, {
        border: colorObj.highlight.background,
      }),
    };
    const hoverObj = {
      hover: highlightObj.highlight,
    };
    this.nodes.get().forEach((v) => {
      if (v.type === nodeType) {
        this.updateNode({
          id: v.id,
          color: Object.assign(colorObj, {
            border: colorObj.background,
          }, highlightObj, hoverObj),
        });
      }
    });
  }

  setColourOnNodeType(baseType, nodeType, colourString) {
    if (colourString === undefined) {
      this.setDefaultNodeColour(baseType, nodeType);
      const t = (nodeType.length) ? nodeType : baseType;
      NodeSettings.setNodeColour(t);
      return;
    }
    if (nodeType.length) {
      NodeSettings.setNodeColour(nodeType, {
        background: colourString,
        highlight: {
          background: colourString,
        },
      });
      this.nodes.get().forEach((v) => {
        if (v.type === nodeType) {
          this.updateNode({
            id: v.id,
            color: {
              background: colourString,
              border: colourString,
              highlight: {
                background: colourString,
                border: colourString,
              },
              hover: {
                background: colourString,
                border: colourString,
              },
            },
          });
        }
      });
    } else {
      NodeSettings.setNodeColour(baseType, {
        background: colourString,
        highlight: {
          background: colourString,
        },
      });
      // If it's a schema node
      this.nodes.get().forEach((v) => {
        if (v.baseType === baseType) {
          this.updateNode({
            id: v.id,
            color: {
              background: colourString,
              border: colourString,
              highlight: {
                background: colourString,
                border: colourString,
              },
              hover: {
                background: colourString,
                border: colourString,
              },
            },
          });
        }
      });
    }
  }

  getNodeOnCoordinates(coordinates) {
    const canvasX = coordinates.x;
    const canvasY = coordinates.y;

    const allNodes = this.nodes.get();
    const arrayLength = allNodes.length;
    for (let i = 0; i < arrayLength; i++) {
      const curNode = allNodes[i];
      const boundingBox = this.network.getBoundingBox(curNode.id);
      if (canvasX <= boundingBox.right && canvasX >= boundingBox.left && canvasY >= boundingBox.top && canvasY <= boundingBox.bottom) {
        return curNode.id;
      }
    }

    return null;
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
     * Check if two nodes (a,b) exist and if they are already connected by an edge.
     */
  alreadyConnected(edge) {
    if (!(this.nodes.get(edge.from) && this.nodes.get(edge.to))) {
      return false;
    }

    const intersection = this.edgesBetweenTwoNodes(edge.from, edge.to);

    return _.contains(
      _.values(intersection)
        .map((x) => {
          const localEdge = this.edges.get(x);
          return Visualiser.matching(edge.from, edge.to, localEdge.to, localEdge.from) && edge.label === localEdge.label;
        }),
      true,
    );
  }

  edgesBetweenTwoNodes(a, b) {
    return _.intersection(this.network.getConnectedEdges(a), this.network.getConnectedEdges(b));
  }

  /**
     * Delete all edges connected to node id
     */
  deleteEdges(id) {
    this.network.getConnectedEdges(id).forEach((edgeId) => {
      this.edges.remove(edgeId);
    });
  }

  static generateLabel(type, attributes, label, baseType) {
    if (baseType === API.RELATIONSHIP || baseType === API.INFERRED_RELATIONSHIP_TYPE) return '';
    if (NodeSettings.getLabelProperties(type).length) {
      return NodeSettings.getLabelProperties(type).reduce((l, x) => {
        let value;
        if (x === 'type') {
          value = type;
          return `${(l.length ? `${l}\n` : l) + value}`;
        }
        value = attributes.filter(attr => attr.type === x).map(a => a.value);
        if (!value) value = '';
        if (Array.isArray(value)) {
          value = value.join(', ');
        } else if (value.length > 40) value = `${value.substring(0, 40)}...`;
        return `${(l.length ? `${l}\n` : l) + x}: ${value}`;
      }, '');
    }
    return label;
  }

  updateNodeLabels(type) {
    this.nodes._data = _.mapObject(this.nodes._data, (v, k) => {
      if (v.type === type) {
        this.updateNode({
          id: k,
          label: Visualiser.generateLabel(type, v.attributes, v.baseLabel, v.baseType),
        });
      }
      return v;
    });
  }


  updateNode(obj) {
    this.nodes.update(obj);
  }

  checkSelectionRectangleStatus(node, eventKeys, param) {
    // If we were drawing rectangle and we click again we stop the drawing and compute selected nodes
    if (this.draggingRect) {
      this.draggingRect = false;
      this.resetRectangle();
      this.network.redraw();
    } else if (eventKeys.ctrlKey && node === undefined) {
      this.draggingRect = true;
      this.startRectangle(param.pointer.canvas.x, param.pointer.canvas.y - this.graphOffsetTop);
    }
  }
}
