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
import HALParser, { URL_REGEX } from '../../../js/HAL/HALParser';
import EngineClient from '../../../js/EngineClient';
import * as Utils from '../../../js/HAL/APIUtils';
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

    this.halParser = new HALParser();

    this.halParser.setNewResource((id, p, a, l, cn) => visualiser.addNode(id, p, a, l, cn));
    this.halParser.setNewRelationship((f, t, l) => visualiser.addEdge(f, t, l));
    this.halParser.setNodeAlreadyInGraph(id => visualiser.nodeExists(id));

    // vars
    this.doubleClickTime = 0;
    this.alphabet = 'abcdefghijklmnopqrstuvwxyz'.split('');
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
      const generatedNode = (nodeObj.baseType === API.GENERATED_RELATION_TYPE);

      EngineClient.request({
        url: nodeObj.href,
      }).then(resp => this.onGraphResponse(resp, node), (err) => {
        this.state.eventHub.$emit('error-message', err.message);
      });
      if (generatedNode) {
        visualiser.deleteNode(node);
      }
    }
  }

  fetchFilteredRelations(href) {
    EngineClient.request({
      url: href,
    }).then(resp => this.onGraphResponse(resp), (err) => {
      this.state.eventHub.$emit('error-message', err.message);
    });
  }

  rightClick(param) {
    const node = param.nodes[0];
    if (node === undefined) {
      return;
    }

    if (param.event.shiftKey) {
      param.nodes.forEach((x) => {
        visualiser.deleteNode(x);
      });
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
      }).then(resp => this.onGraphResponseExplore(resp, nodeObj.id), (err) => {
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
          // Show node properties on node panel.
      const ontologyProps = {
        id: nodeObj.id,
        type: nodeObj.type,
        baseType: nodeObj.baseType,
      };

      const nodeResources = CanvasHandler.prepareResources(nodeObj.properties);
      const nodeLabel = visualiser.getNodeLabel(node);
      this.state.eventHub.$emit('show-node-panel', ontologyProps, nodeResources, nodeLabel);
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
        EngineClient.graqlHAL(query).then(resp => this.onGraphResponse(resp), (err) => {
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
      EngineClient.graqlHAL(queryToExecute).then((resp, nodeId) => this.onGraphResponse(resp, nodeId), (err) => {
        this.state.eventHub.$emit('error-message', err.message);
      });
    }
  }


  onLoadOntology(type) {
    const querySub = `match $x sub ${type};`;
    EngineClient.graqlHAL(querySub).then((resp, nodeId) => this.onGraphResponse(resp, nodeId), (err) => {
      this.state.eventHub.$emit('error-message', err.message);
    });
  }
  // //----------- Render Engine responses ------------------ ///

  onGraphResponseAnalytics(resp) {
    const responseObject = JSON.parse(resp).response;
    this.state.eventHub.$emit('analytics-string-response', responseObject);
  }

  onGraphResponse(resp, nodeId) {
    const responseObject = JSON.parse(resp).response;
    if (!this.halParser.parseResponse(responseObject, false, false, nodeId)) {
      this.state.eventHub.$emit('warning-message', 'No results were found for your query.');
    } else if (nodeId) {
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
    visualiser.fitGraphToWindow();
  }

  onGraphResponseExplore(resp, nodeId) {
    if (!this.halParser.parseResponse(JSON.parse(resp).response, false, true, nodeId)) {
      this.state.eventHub.$emit('warning-message', 'No results were found for your query.');
    }
    visualiser.fitGraphToWindow();
  }

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

  /**
   * Prepare the list of resources to be shown in the right div panel
   * It sorts them alphabetically and then check if a resource value is a URL
   */
  static prepareResources(originalObject) {
    if (originalObject == null) return {};
    return Object.keys(originalObject).sort().reduce(
          // sortedObject is the accumulator variable, i.e. new object with sorted keys
          // k is the current key
          (sortedObject, k) => {
              // Add 'href' field to the current object, it will be set to TRUE if it contains a valid URL, FALSE otherwise
            const currentResourceWithHref = Object.assign({}, originalObject[k], {
              href: CanvasHandler.validURL(originalObject[k].label),
            });
            return Object.assign({}, sortedObject, {
              [k]: currentResourceWithHref,
            });
          }, {});
  }
  static validURL(str) {
    const pattern = new RegExp(URL_REGEX, 'i');
    return pattern.test(str);
  }

}
