import Visualiser from './Visualiser';
/*
* Creates a new object that can be used to interact with the visualiser graph
* given a specific container(DOM element)
*/
function createFacade(prototype, state, container, style) {
  const facade = Object.assign(Object.create(prototype), state);

  facade.style = style;
  facade.container = container;
  facade.container.visualiser = Visualiser.createVisualiser();
  facade.container.visualiser.render(container);

  return facade;
}

/**
 * Delete all or some (based on label) edges connected to a specific node.
 * @param {String} nodeId id of node from which remove all the edges
 * @param {String} label optional label used to filter edges to be deleted
 */
function deleteEdgesOnNode(nodeId, label) {
  const edgesIds = this.container.visualiser.edgesConnectedToNode(nodeId);
  const idsToDelete = (label) ? this.container.visualiser.getEdge(edgesIds)
    .filter(x => x.label === label).map(x => x.id) : edgesIds;
  idsToDelete.forEach((edgeId) => { this.container.visualiser.deleteEdge(edgeId); });
}

/*
   ------------------  CANVAS BASIC OPERATIONS   ------------------------
 */


/**
 * Method used to remove nodes and all their adjacent edges from graph
 * @param {string[]} nodeIds array of nodeIds to be removed from canvas.
 */
function deleteFromCanvas(nodeIds) {
  nodeIds.forEach((id) => {
    this.deleteEdgesOnNode(id);
    this.container.visualiser.deleteNode(id);
  });
}

/**
 * Method used to add new nodes and edges to the canvas
 * @param {*} data Object containing array of nodes and edges to be added to the canvas.
 */

function addToCanvas(data) {
  data.nodes.forEach((node) => {
    const styledNode = Object.assign(node, this.style.computeNodeStyle(node));
    this.container.visualiser.addNode(styledNode);
  });

  data.edges.forEach((edge) => {
    if (!edge.color) { Object.assign(edge, this.style.computeEdgeStyle(edge)); }
    this.container.visualiser.addEdge(edge);
  });
}

function addNode(node) {
  const styledNode = Object.assign(node, this.style.computeNodeStyle(node));
  this.container.visualiser.addNode(styledNode);
}

function getNode(nodeId) {
  return this.container.visualiser.getNode(nodeId);
}

function updateNode(node) {
  return this.container.visualiser.updateNode(node);
}

function getAllNodes() {
  return this.container.visualiser.getNode();
}

function getAllEdges() {
  return this.container.visualiser.getEdge();
}

/**
 * Clear all the nodes and edges from canvas.
 */
function resetCanvas() {
  this.container.visualiser.clearGraph();
}

function registerEventHandler(event, fn) {
  this.container.visualiser.getNetwork().on(event, fn);
}

function fitGraphToWindow() { this.container.visualiser.getNetwork().fit({ animation: { easingFunction: 'easeInOutQuad', duration: 500 } }); }

function getNetwork() {
  return this.container.visualiser.getNetwork();
}
const state = {
  container: undefined,
};

const prototype = {
  resetCanvas,
  deleteFromCanvas,
  deleteEdgesOnNode,
  addToCanvas,
  addNode,
  registerEventHandler,
  fitGraphToWindow,
  getAllNodes,
  getNode,
  getAllEdges,
  updateNode,
  getNetwork,
};

export default {
  initVisualiser(container, style) {
    return createFacade(prototype, state, container, style);
  },
};
