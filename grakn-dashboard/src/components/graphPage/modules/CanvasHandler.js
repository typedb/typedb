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
import Visualiser from '../../../js/visualiser/Visualiser';
import HALParser from '../../../js/HAL/HALParser';
import * as Utils from '../../../js/HAL/APIUtils';
import EngineClient from '../../../js/EngineClient';
import User from '../../../js/User';
import * as API from '../../../js/util/HALTerms';

export default class CanvasHandler {

  constructor(graphPageState) {
    this.state = graphPageState;
    // TODO: make a more clear division of functions used to draw selection rectangle
    this.graphOffsetTop = undefined;
    window.visualiser = new Visualiser();

    visualiser.setCallbackOnEvent('click', param => this.singleClick(param));
    visualiser.setCallbackOnEvent('doubleClick', param => this.doubleClick(param));
    visualiser.setCallbackOnEvent('oncontext', param => this.rightClick(param));
    visualiser.setCallbackOnEvent('hold', param => this.holdOnNode(param));
    visualiser.setCallbackOnEvent('hoverNode', param => this.hoverNode(param));
    visualiser.setCallbackOnEvent('blurNode', param => this.blurNode(param));
    visualiser.setCallbackOnEvent('dragStart', param => this.onDragStart(param));

    // vars
    this.doubleClickTime = 0;
  }

  renderGraph(graphElement, graphOffsetTop) {
    this.graphOffsetTop = graphOffsetTop;
    visualiser.render(graphElement);
  }

  clearGraph() {
    visualiser.clearGraph();
  }

  // //////////////////////////////////////////////////// ----------- Graph mouse interactions ------------ ///////////////////////////////////////////////////////////

  holdOnNode(param) {
    visualiser.network.unselectAll();
    const node = visualiser.getNodeOnCoordinates(param.pointer.canvas);
    if (node === null) return;
    this.state.eventHub.$emit('show-label-panel', visualiser.getAllNodeProperties(node), visualiser.getNodeType(node), node);
  }

  doubleClick(param) {
    this.doubleClickTime = new Date();
    const node = param.nodes[0];
    if (node === undefined) {
      return;
    }

    const eventKeys = param.event.srcEvent;
    const nodeObj = visualiser.getNode(node);

    if (eventKeys.shiftKey) {
      this.requestExplore(nodeObj);
    } else {
      EngineClient.request({
        url: nodeObj.href,
      }).then(resp => this.onGraphResponse(resp, false, false, node), (err) => {
        this.state.eventHub.$emit('error-message', err.message);
      });
      if (nodeObj.baseType === API.GENERATED_RELATION_TYPE) {
        visualiser.deleteNode(node);
      }
    }
  }

  rightClick(param) {
    const node = param.nodes[0];
    if (node === undefined) { return; }

    if (param.event.shiftKey) {
      param.nodes.forEach((x) => { visualiser.deleteNode(x); });
    }
  }

  hoverNode(param) {
    this.state.eventHub.$emit('hover-node', param);
  }

  blurNode() {
    this.state.eventHub.$emit('blur-node');
  }

  requestExplore(nodeObj) {
    if (nodeObj.explore) {
      EngineClient.request({
        url: nodeObj.explore,
      }).then(resp => this.onGraphResponse(resp, false, true, nodeObj.id), (err) => {
        this.state.eventHub.$emit('error-message', err.message);
      });
    }
  }

  leftClick(param) {
    const node = param.nodes[0];
    const eventKeys = param.event.srcEvent;
    const clickType = param.event.type;

      // If it is a long press on node: return and onHold() method will handle the event.
    if (clickType !== 'tap') {
      return;
    }

      // Check if we need to start or stop drawing the selection rectangle
    this.checkSelectionRectangleStatus(node, eventKeys, param);

    if (node === undefined) {
      return;
    }

    const nodeObj = visualiser.getNode(node);

    if (eventKeys.shiftKey) {
      this.requestExplore(nodeObj);
    } else {
      this.state.eventHub.$emit('show-node-panel', nodeObj);
    }
  }

  singleClick(param) {
      // Everytime the user clicks on canvas we clear the context-menu and tooltip
    this.state.eventHub.$emit('close-context');
    this.state.eventHub.$emit('close-tooltip');

    const t0 = new Date();
    const threshold = 200;
      // all this fun to be able to distinguish a single click from a double click
    if (t0 - this.doubleClickTime > threshold) {
      setTimeout(() => {
        if (t0 - this.doubleClickTime > threshold) {
          this.leftClick(param);
        }
      }, threshold);
    }
  }

  onDragStart(params) {
    const eventKeys = params.event.srcEvent;
    visualiser.draggingNode = true;
    this.state.eventHub.$emit('close-tooltip');
      // If ctrl key is pressed while dragging node/nodes we also unlock and drag the connected nodes
    if (eventKeys.ctrlKey) {
      const neighbours = [];
      params.nodes.forEach((node) => {
        neighbours.push(...visualiser.network.getConnectedNodes(node));
        neighbours.push(node);
      });
      visualiser.network.selectNodes(neighbours);
      visualiser.releaseNodes(neighbours);
    } else {
      visualiser.releaseNodes(params.nodes);
    }
  }

  // ----- End of graph interactions ------- //

  onClickSubmit(query) {
    if (query.includes('aggregate')) {
          // Error message until we will not properly support aggregate queries in graph page.
      this.state.eventHub.$emit('error-message', 'Invalid query: \'aggregate\' queries are not allowed from the Graph page. Please use the Console page.');
      return;
    }

    if (query.startsWith('compute')) {
      // If analytics query contains path we execute a HAL request
      if (query.includes('path')) {
        EngineClient.graqlHAL(query).then(resp => this.onGraphResponse(resp, false, false), (err) => {
          this.state.eventHub.$emit('error-message', err.message);
        });
      } else {
        EngineClient.graqlAnalytics(query).then(resp => this.onGraphResponseAnalytics(resp), (err) => {
          this.state.eventHub.$emit('error-message', err.message);
        });
      }
    } else {
      let queryToExecute = query.trim();

      if (!(query.includes('offset')) && !(query.includes('delete'))) { queryToExecute = `${queryToExecute} offset 0;`; }
      if (!(query.includes('limit')) && !(query.includes('delete'))) { queryToExecute = `${queryToExecute} limit ${User.getQueryLimit()};`; }
      this.state.eventHub.$emit('inject-query', queryToExecute);
      EngineClient.graqlHAL(queryToExecute).then((resp, nodeId) => this.onGraphResponse(resp, false, false, nodeId), (err) => {
        this.state.eventHub.$emit('error-message', err.message);
      });
    }
  }


  onLoadOntology(type) {
    const querySub = `match $x sub ${type};`;
    EngineClient.graqlHAL(querySub).then(resp => this.onGraphResponse(resp, false, false), (err) => {
      this.state.eventHub.$emit('error-message', err.message);
    });
  }
  // //----------- Render Engine responses ------------------ ///

  onGraphResponseAnalytics(resp) {
    const responseObject = JSON.parse(resp).response;
    this.state.eventHub.$emit('analytics-string-response', responseObject);
  }

  onGraphResponse(resp, showIsa, showResources, nodeId) {
    const responseObject = JSON.parse(resp).response;
    const parsedResponse = HALParser.parseResponse(responseObject, showIsa);

    if (!parsedResponse.nodes.length) {
      this.state.eventHub.$emit('warning-message', 'No results were found for your query.');
      return;
    }
    
    const filteredNodes = this.filterNodesToRender(responseObject, parsedResponse, showResources);

    // Collect instances from filteredNodes to lazy load their resources.
    const instances = filteredNodes.map(x => x.properties).filter(node => ((node.baseType === API.ENTITY || node.baseType === API.RELATION || node.baseType === API.RULE) && (!visualiser.nodeExists(node.id))));

    filteredNodes.forEach(node => visualiser.addNode(node.properties, node.resources, node.links, nodeId));
    parsedResponse.edges.forEach(edge => visualiser.addEdge(edge.from, edge.to, edge.label));

    this.loadInstancesResources(0, instances);

    if (nodeId) this.updateNodeHref(nodeId, responseObject);

    visualiser.fitGraphToWindow();
  }

  filterNodesToRender(responseObject, parsedResponse, showResources) {
    const dataArray = (Array.isArray(responseObject)) ? responseObject : [responseObject];
    // Populate map containing all the first level objects returned in the response, they MUST be added to the graph.
    const firstLevelNodes = dataArray.reduce((accumulator, current) => Object.assign(accumulator, { [current._id]: true }), {});

    // Add embedded object to the graph only if one of the following is satisfied:
    // - the current node is not a RESOURCE_TYPE || showResources is set to true
    // - the current node is already drawn in the graph
    // - the current node is contained in the response as first level object (not embdedded)
    //    if it's contained in firstLevelNodes it means it MUST be drawn and so all the edges pointing to it.

    return parsedResponse.nodes.filter(node => (((node.properties.baseType !== API.RESOURCE_TYPE)
          && (node.properties.baseType !== API.RESOURCE)
          || showResources)
          || (firstLevelNodes[node.properties.id])
          || visualiser.nodeExists(node.properties.id)));
  }

  updateNodeHref(nodeId, responseObject) {
     // When a nodeId is provided is because the user double-clicked on a node, so we need to update its href
      // which will contain a new value for offset
      // Check if the node still in the Dataset, if not (generated relation), don't update href
    if (visualiser.getNode(nodeId) && ('_links' in responseObject)) {
      visualiser.updateNode({
        id: nodeId,
        href: responseObject._links.self.href,
      });
    }
  }

  // --------------------------------------- LAZY LOAD RESOURCESSSSS  ------------------------

  loadInstancesResources(start, instances) {
    const batchSize = 50;
    const promises = [];

    // Add a batchSize number of requests to the promises array
    for (let i = start; i < start + batchSize; i++) {
      if (i >= instances.length) {
        // When all the requests are loaded in promises flush the remaining ones and update labels on nodes
        this.flushPromises(promises).then(() => visualiser.refreshLabels(instances));
        return;
      }
      promises.push(EngineClient.request({
        url: instances[i].explore,
      }));
    }
    this.flushPromises(promises).then(() => this.loadInstancesResources(start + batchSize, instances));
  }

  flushPromises(promises) {
    return Promise.all(promises).then((responses) => {
      responses.forEach((resp) => {
        const respObj = JSON.parse(resp).response;
        // Check if some of the resources attached to this node are already drawn in the graph:
        // if a resource is already in the graph (because explicitly asked for (e.g. all relations with weight > 0.5 ))
        // we need to draw the edges connecting this node to the resource node.
        this.onGraphResponse(resp, false, false);
        visualiser.updateNodeResources(respObj[API.KEY_ID], Utils.extractResources(respObj));
      });
      visualiser.flushUpdates();
    });
  }
  // --------------------------------------------------------------- End of LAZY LOAD RESOURCES ----------------------------------------------

  checkSelectionRectangleStatus(node, eventKeys, param) {
      // If we were drawing rectangle and we click again we stop the drawing and compute selected nodes
    if (visualiser.draggingRect) {
      visualiser.draggingRect = false;
      visualiser.resetRectangle();
      visualiser.network.redraw();
    } else if (eventKeys.ctrlKey && node === undefined) {
      visualiser.draggingRect = true;
      visualiser.startRectangle(param.pointer.canvas.x, param.pointer.canvas.y - this.graphOffsetTop);
    }
  }

  fetchFilteredRelations(href) {
    EngineClient.request({
      url: href,
    }).then(resp => this.onGraphResponse(resp, false, false), (err) => {
      this.state.eventHub.$emit('error-message', err.message);
    });
  }

}
