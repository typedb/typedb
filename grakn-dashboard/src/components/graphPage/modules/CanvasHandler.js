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
/* @flow */

import HALParser from '../../../js/HAL/HALParser';
import * as Utils from '../../../js/HAL/APIUtils';
import EngineClient from '../../../js/EngineClient';
import User from '../../../js/User';
import * as API from '../../../js/util/HALTerms';
import { EventHub } from '../../../js/state/graphPageState';
import CanvasEvents from './CanvasEvents';


function clearGraph() {
  visualiser.clearGraph();
}


function onClickSubmit(query:string) {
  if (query.includes('aggregate')) {
          // Error message until we will not properly support aggregate queries in graph page.
    EventHub.$emit('error-message', '{"exception":"Invalid query: \'aggregate\' queries are not allowed from the Graph page. \\nPlease use the Console page."}');
    return;
  }

  if (query.startsWith('compute')) {
      // If analytics query contains path we execute a HAL request
    if (query.includes('path')) {
      EngineClient.graqlHAL(query).then(resp => onGraphResponse(resp, false, false), (err) => {
        EventHub.$emit('error-message', err.message);
      });
    } else {
      EngineClient.graqlAnalytics(query).then(resp => onGraphResponseAnalytics(resp), (err) => {
        EventHub.$emit('error-message', err.message);
      });
    }
  } else {
    let queryToExecute = query.trim();

    if (!(query.includes('offset')) && !(query.includes('delete'))) { queryToExecute = `${queryToExecute} offset 0;`; }
    if (!(query.includes('limit')) && !(query.includes('delete'))) { queryToExecute = `${queryToExecute} limit ${User.getQueryLimit()};`; }
    EventHub.$emit('inject-query', queryToExecute);
    EngineClient.graqlHAL(queryToExecute).then((resp, nodeId) => onGraphResponse(resp, false, false, nodeId), (err) => {
      EventHub.$emit('error-message', err.message);
    });
  }
}

function onLoadOntology(type:string) {
  const querySub = `match $x sub ${type};`;
  EngineClient.graqlHAL(querySub).then(resp => onGraphResponse(resp, false, false), (err) => {
    EventHub.$emit('error-message', err.message);
  });
}

function onGraphResponseAnalytics(resp:string) {
  const responseObject = JSON.parse(resp);
  EventHub.$emit('analytics-string-response', responseObject);
}

function filterNodesToRender(responseObject:Object|Object[], parsedResponse:Object, showResources:boolean) {
  const dataArray = (Array.isArray(responseObject)) ? responseObject : [responseObject];
    // Populate map containing all the first level objects returned in the response, they MUST be added to the graph.
  const firstLevelNodes = dataArray.reduce((accumulator, current) => Object.assign(accumulator, { [current._id]: true }), {});

    // Add embedded object to the graph only if one of the following is satisfied:
    // - the current node is not a RESOURCE_TYPE || showResources is set to true
    // - the current node is already drawn in the graph
    // - the current node is contained in the response as first level object (not embdedded)
    //    if it's contained in firstLevelNodes it means it MUST be drawn and so all the edges pointing to it.

  return parsedResponse.nodes
          .filter(node=> !node.properties.implicit)
          .filter(node => (((node.properties.baseType !== API.RESOURCE_TYPE)
          && (node.properties.baseType !== API.RESOURCE)
          || showResources)
          || (firstLevelNodes[node.properties.id])
          || visualiser.nodeExists(node.properties.id)));
}

function updateNodeHref(nodeId:string, responseObject:Object) {
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

function loadInstancesResources(start:number, instances:Object[]) {
  const batchSize = 50;
  const promises = [];

    // Add a batchSize number of requests to the promises array
  for (let i = start; i < start + batchSize; i++) {
    if (i >= instances.length) {
        // When all the requests are loaded in promises flush the remaining ones and update labels on nodes
      flushPromises(promises).then(() => visualiser.refreshLabels(instances));
      return;
    }
    promises.push(EngineClient.request({
      url: instances[i].explore,
    }));
  }
  flushPromises(promises).then(() => loadInstancesResources(start + batchSize, instances));
}

function flushPromises(promises:Object[]) {
  return Promise.all(promises.map(softFail)).then((responses) => {
    responses.filter(x => x.success).map(x => x.result).forEach((resp) => {
      const respObj = JSON.parse(resp);
      // Check if some of the resources attached to this node are already drawn in the graph:
      // if a resource is already in the graph (because explicitly asked for (e.g. all relations with weight > 0.5 ))
      // we need to draw the edges connecting this node to the resource node.
      onGraphResponse(resp, false, false);
      visualiser.updateNodeResources(respObj[API.KEY_ID], Utils.extractResources(respObj));
    });
    visualiser.flushUpdates();
  });
}

// Function used to avoid the fail-fast behaviour of Promise.all:
// map all the promises results to objects wheter they fail or not.
function softFail(promise) {
  return promise
    .then(result => ({ success: true, result }))
    .catch(error => ({ success: false, error }));
}

function linkResourceOwners(instances) {
  instances.forEach((resource) => {
    EngineClient.request({
      url: resource.properties.href,
    }).then((resp) => {
      const responseObject = JSON.parse(resp);
      const parsedResponse = HALParser.parseResponse(responseObject, false);
      parsedResponse.edges.forEach(edge => visualiser.addEdge(edge.from, edge.to, edge.label));
    });
  });
}


/*
* Public functions
*/

function initialise(graphElement:Object) {
  EventHub.$on('clear-page', () => clearGraph());
  EventHub.$on('click-submit', query => onClickSubmit(query));
  EventHub.$on('load-ontology', type => onLoadOntology(type));
  CanvasEvents.registerCanvasEvents();
  // Render visualiser only after having registered all the events handlers.
  visualiser.render(graphElement);
}

function onGraphResponse(resp:string, showIsa:boolean, showResources:boolean, nodeId:?string) {
  const responseObject = JSON.parse(resp);
  const parsedResponse = HALParser.parseResponse(responseObject, showIsa);

  if (!parsedResponse.nodes.length) {
    EventHub.$emit('warning-message', 'No results were found for your query.');
    return;
  }

  const filteredNodes = filterNodesToRender(responseObject, parsedResponse, showResources);

    // Collect instances from filteredNodes to lazy load their resources.
  const instances = filteredNodes
                    .map(x => x.properties)
                    .filter(node => ((node.baseType === API.ENTITY || node.baseType === API.RELATION || node.baseType === API.RULE) && (!visualiser.nodeExists(node.id))));

  filteredNodes.forEach(node => visualiser.addNode(node.properties, node.resources, node.links, nodeId));
  parsedResponse.edges.forEach(edge => visualiser.addEdge(edge.from, edge.to, edge.label));

  loadInstancesResources(0, instances);

  // Check if there are resources and make sure they are linked to their owners (if any already drawn in the graph)
  linkResourceOwners(filteredNodes.filter(node => ((node.properties.baseType === API.RESOURCE))));

  if (nodeId) updateNodeHref(nodeId, responseObject);

  visualiser.fitGraphToWindow();
}

function fetchFilteredRelations(href:string) {
  EngineClient.request({
    url: href,
  }).then(resp => onGraphResponse(resp, false, false), (err) => {
    EventHub.$emit('error-message', err.message);
  });
}

function loadResourceOwners(resourceId:string) {
  EngineClient.request({
    url: resourceId,
  }).then(resp => onGraphResponse(resp, false, true));
}


export default { initialise, onGraphResponse, fetchFilteredRelations, loadResourceOwners };
