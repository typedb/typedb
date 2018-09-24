/* eslint no-underscore-dangle: ["error", { "allowAfterThis": true }] */

import vis from 'vis';
import _ from 'underscore';

import Settings from './Settings';
import * as eventsHandlers from './Events';

function render(container) {
  this._nodes = new vis.DataSet([]);
  this._edges = new vis.DataSet([]);
  this._network = new vis.Network(
    container, {
      nodes: this._nodes,
      edges: this._edges,
    },
    this._options,
  );

  this._network.on('dragEnd', params => this.onDragEnd(params));
  this._network.on('dragStart', params => this.onDragStart(params));
  this._network.on('hoverNode', params => this.onHoverNode(params));
  this._network.on('blurNode', params => this.onBlurNode(params));
  this._network.on('selectNode', params => this.onSelectNode(params));
  this._network.on('deselectNode', params => this.onDeselectNode(params));
  this._network.on('oncontext', params => this.onContext(params));
  this._network.on('click', params => this.onClick(params));
}


function alreadyConnected(a, b, label) {
  if (!(this.nodeExists(a) && this.nodeExists(b))) { return false; }

  const intersection = this.edgesBetweenTwoNodes(a, b);

  return _.contains(_.values(intersection).map(x => this._edges.get(x).hiddenLabel === label), true);
}


function clearGraph() {
  this._nodes.clear();
  this._edges.clear();
  this._network.setData({
    nodes: this._nodes,
    edges: this._edges,
  });
}

/*
* Edges methods
*/

/**
 * Retrieve edge/s with given id/s
 * @param {String | String[]} edgeId sigle or array of ids
 */
function getEdge(edgeIdOrIds) {
  return this._edges.get(edgeIdOrIds);
}

function edgesBetweenTwoNodes(a, b) {
  return _.intersection(this._network.getConnectedEdges(a), this._network.getConnectedEdges(b));
}

function addEdge(edge) {
  if (this.nodeExists(edge.from) && this.nodeExists(edge.to)
        && !this.alreadyConnected(edge.from, edge.to, edge.label)) {
    this._edges.add(Object.assign(edge, { label: '', hiddenLabel: edge.label }));
    this.checkParallelEdges(edge.from, edge.to);
  }
}


function checkParallelEdges(from, to) {
  const common = this.edgesBetweenTwoNodes(from, to);
  if (common.length < 2) return;
  let ness = 1;
  common.forEach((edgeId, i) => {
    let round = 0.1;
    round = (i % 2 === 0) ? (round * (ness)) : -(round * (ness));
    // Use roundness if using type: 'curvedCW'
    const obj = { smooth: { enabled: true, type: 'dynamic', roundness: round } };
    this.updateEdge(Object.assign({ id: edgeId }, obj));
    if (i % 2 !== 0) ness += 1;
  });
}


function updateEdge(edgeObj) {
  this._edges.update(edgeObj);
}

function deleteEdge(edgeId) {
  this._edges.remove(edgeId);
}
/*
* Nodes methods
*/

function deleteNode(nodeId) {
  this._nodes.remove(nodeId);
}

function nodeExists(nodeId) {
  return (this._nodes.get(nodeId) !== null);
}

function addNode(node) {
  if (!this.nodeExists(node.id)) {
    this._nodes.add(node);
  }
}

function updateNode(nodeObj) {
  this._nodes.update(nodeObj);
}

function getNode(nodeId) {
  return this._nodes.get(nodeId);
}

function highlightNode(nodeId) {
  const node = this.getNode(nodeId);
  this.updateNode({
    id: nodeId,
    color: { background: node.colorClone.hover.background, border: node.colorClone.hover.border },
  });
}

function removeHighlightNode(nodeId) {
  const node = this.getNode(nodeId);
  if (node === null) return;
  this.updateNode({
    id: nodeId,
    color: node.colorClone,
  });
}

/**
 * Retrieve edges ids connected to a given node
 * @param {String} nodeId id of node
 * @return array of edgeIds of the edges connected to this node.
 */
function edgesConnectedToNode(nodeId) {
  return this._network.getConnectedEdges(nodeId);
}

function getNetwork() {
  return this._network;
}

const state = {
  _nodes: undefined,
  _edges: undefined,
  _network: {},
  _options: Settings.networkOptions,
};

const prototype = {
  render,
  nodeExists,
  addNode,
  deleteNode,
  getNode,
  highlightNode,
  removeHighlightNode,
  addEdge,
  getEdge,
  deleteEdge,
  updateEdge,
  clearGraph,
  alreadyConnected,
  edgesBetweenTwoNodes,
  edgesConnectedToNode,
  updateNode,
  getNetwork,
  checkParallelEdges,
};


export default {
  createVisualiser() {
    const extendedPrototype = Object.assign(prototype, eventsHandlers);
    return Object.assign(Object.create(extendedPrototype), state);
  },
};
