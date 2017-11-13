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
import * as API from '../../../js/util/HALTerms';
import { EventHub } from '../../../js/state/graphPageState';
import CanvasEvents from './CanvasEvents';

function clearGraph() {
  visualiser.clearGraph();
}


function onClickSubmit(query:string) {
  if (query.includes('aggregate')
          || (query.includes('compute') && query.includes('degrees'))
      || (query.includes('compute') && query.includes('cluster'))) {// Error message until we will not properly support aggregate queries in graph page.
    EventHub.$emit('error-message', '{"exception":"Invalid query: \\n \'aggregate\' queries \\n \'compute degrees\' \\n \'compute cluster\' \\nare not allowed from the Graph page. \\n \\nPlease use the Console page."}');
    return;
  }

  if (query.startsWith('compute')&& !query.includes('path')) {

      EngineClient.graqlAnalytics(query).then(resp => onGraphResponseAnalytics(resp), (err) => {
        EventHub.$emit('error-message', err.message);
      });

  } else {
    EngineClient.graqlHAL(query)
    .then((resp, nodeId) => onGraphResponse(resp, false, false, false, nodeId))
    .then((instances) => { if(instances) loadInstancesAttributes(0, instances); })
    .catch((err) => { EventHub.$emit('error-message', err.message); });
  }
}

function onLoadSchema(type: string) {
  const querySub = `match $x sub ${type}; get;`;
  EngineClient.graqlHAL(querySub).then(resp => onGraphResponse(resp, false, false, false), (err) => {
    EventHub.$emit('error-message', err.message);
  });
}

function onGraphResponseAnalytics(resp: string) {
  const responseObject = JSON.parse(resp);
  EventHub.$emit('analytics-string-response', responseObject);
}

function flatten<T>(array: T[][]): T[] {
  return array.reduce((x, y) => x.concat(y), []);
}

function filterNodesToRender(responseObject: Object | Object[], parsedResponse: Object, showAttributes: boolean, isExplore: boolean) {
  const dataArray = (Array.isArray(responseObject)) ? responseObject : [responseObject];
  // Populate map containing all the first level objects returned in the response, they MUST be added to the graph.
  const firstLevelNodes = dataArray.reduce((accumulator, current) => Object.assign(accumulator, { [current._id]: true }), {});

  const rolePlayersOf = node => node._embedded ? flatten(Object.values(node._embedded)) : [];
  const isRelationship = node => node._baseType == API.RELATIONSHIP || node._baseType == API.INFERRED_RELATIONSHIP_TYPE;
  const rolePlayerNodes = (isExplore) ? new Set() : new Set(flatten(dataArray.filter(isRelationship).map(rolePlayersOf)).map(node => node._id));

  // Add embedded object to the graph only if one of the following is satisfied:
  // - the current node is not a ATTRIBUTE_TYPE || showAttributes is set to true
  // - the current node is already drawn in the graph
  // - the current node is contained in the response as first level object (not embdedded)
  //    if it's contained in firstLevelNodes it means it MUST be drawn and so all the edges pointing to it.
  // - the current node is a role-player of a first level node

  const disallowedBaseTypes = showAttributes ? [] : [API.ATTRIBUTE, API.ATTRIBUTE_TYPE];

  const notImplicit = node => !node.properties.implicit;
  const baseTypeIsAllowed = node => !disallowedBaseTypes.includes(node.properties.baseType);
  const isFirstLevel = node => firstLevelNodes[node.properties.id];
  const isRolePlayer = node => rolePlayerNodes.has(node.properties.id);

  return parsedResponse.nodes
    .filter(notImplicit)
    .filter(node => baseTypeIsAllowed(node) || isFirstLevel(node) || isRolePlayer(node) || visualiser.nodeExists(node.properties.id));
}

function updateNodeHref(nodeId: string, responseObject: Object) {
  // When a nodeId is provided, it is because the user double-clicked on a node, so we need to update its href
  // which will contain a new value for offset.
  // If the node does not exist in the Dataset don't update href
  if (visualiser.getNode(nodeId) && ('_links' in responseObject)) {
    visualiser.updateNode({
      id: nodeId,
      href: responseObject._links.self.href,
    });
  }
}

function loadInstancesAttributes(start: number, instances: Object[]) {
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
  flushPromises(promises).then(() => loadInstancesAttributes(start + batchSize, instances));
}

function flushPromises(promises: Object[]) {
  return Promise.all(promises.map(softFail)).then((responses) => {
    responses.filter(x => x.success).map(x => x.result).forEach((resp) => {
      const respObj = JSON.parse(resp);
      // Check if some of the attributes attached to this node are already drawn in the graph:
      // if a attribute is already in the graph (because explicitly asked for (e.g. all relationships with weight > 0.5 ))
      // we need to draw the edges connecting this node to the attribute node.
      onGraphResponse(resp, false, false, true);
      visualiser.updateNodeAttributes(respObj[API.KEY_ID], Utils.extractAttributes(respObj));
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

/*
* Public functions
*/

function initialise(graphElement: Object) {
  EventHub.$on('clear-page', () => clearGraph());
  EventHub.$on('click-submit', query => onClickSubmit(query));
  EventHub.$on('load-schema', type => onLoadSchema(type));
  CanvasEvents.registerCanvasEvents();
  // Render visualiser only after having registered all the events handlers.
  visualiser.render(graphElement);
}

function onGraphResponse(resp: string, showIsa: boolean, showAttributes: boolean, isExplore: boolean ,nodeId:?string) {
  const responseObject = JSON.parse(resp);
  const parsedResponse = HALParser.parseResponse(responseObject, showIsa);

  if (!parsedResponse.nodes.length) {
    EventHub.$emit('warning-message', 'No results were found for your query.');
    return;
  }

  const filteredNodes = filterNodesToRender(responseObject, parsedResponse, showAttributes, isExplore);

  // Collect instances from filteredNodes to lazy load their attributes.
  const instances = filteredNodes
    .map(x => x.properties)
    .filter(node => ((node.baseType === API.ENTITY || node.baseType === API.RELATIONSHIP || node.baseType === API.RULE)));

  filteredNodes.forEach(node => visualiser.addNode(node.properties, node.attributes, node.links, nodeId));
  parsedResponse.edges.forEach(edge => visualiser.addEdge(edge.from, edge.to, edge.label));

  if (nodeId) updateNodeHref(nodeId, responseObject);

  visualiser.fitGraphToWindow();

  return instances;
}

function fetchFilteredRelationships(href: string) {
  EngineClient.request({
    url: href,
  }).then(resp => onGraphResponse(resp, false, false, false), (err) => {
    EventHub.$emit('error-message', err.message);
  });
}

function loadAttributeOwners(attributeId: string) {
  EngineClient.request({
    url: attributeId,
  }).then(resp => onGraphResponse(resp, false, true, false));
}


export default { initialise, onGraphResponse, fetchFilteredRelationships, loadAttributeOwners, loadInstancesAttributes };
