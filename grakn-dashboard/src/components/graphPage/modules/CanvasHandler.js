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

import Parser from '../../../js/Parser/Parser';
import EngineClient from '../../../js/EngineClient';
import { EventHub } from '../../../js/state/graphPageState';
import CanvasEvents from './CanvasEvents';
import Visualiser from '../../../js/visualiser/Visualiser';

function clearGraph() {
  visualiser.clearGraph();
}


function onClickSubmit(query: string) {
  if (query.includes('aggregate')
    || (query.includes('compute') && query.includes('degrees'))
    || (query.includes('compute') && query.includes('cluster'))) { // Error message until we will not properly support aggregate queries in graph page.
    EventHub.$emit('error-message', '{"exception":"Invalid query: \\n \'aggregate\' queries \\n \'compute degrees\' \\n \'compute cluster\' \\nare not allowed from the Graph page. \\n \\nPlease use the Console page."}');
    return;
  }

  if (query.startsWith('compute') && !query.includes('path')) {
    EngineClient.graqlAnalytics(query)
      .then(resp => onGraphResponseAnalytics(resp))
      .catch((err) => { EventHub.$emit('error-message', err); });
  } else {
    EngineClient.graqlQuery(query)
      .then((resp, nodeId) => onGraphResponse(resp))
      .catch((err) => { EventHub.$emit('error-message', err); });
  }
}

function onLoadSchema(type: string) {
  const querySub = `match $x sub ${type}; get;`;
  EngineClient.graqlQuery(querySub)
    .then(resp => onGraphResponse(resp))
    .catch((err) => { EventHub.$emit('error-message', err); });
}

function onGraphResponseAnalytics(resp: string) {
  const responseObject = JSON.parse(resp);
  EventHub.$emit('analytics-string-response', responseObject);
}

function filterNodes(nodes) { return nodes.filter(x => !x.implicit).filter(x => !x.abstract); }

function initialise(graphElement: Object) {
  EventHub.$on('clear-page', () => clearGraph());
  EventHub.$on('click-submit', query => onClickSubmit(query));
  EventHub.$on('load-schema', type => onLoadSchema(type));
  CanvasEvents.registerCanvasEvents();
  // Render visualiser only after having registered all the events handlers.
  visualiser.render(graphElement);
}

// Once all the attributes of a node are loaded, check if one of these attrs is drawn on the graph
// If that's the case, draw edge between concept and attributes nodes.
function linkNodeToExplicitAttributeNodes(nodeId, attributes) {
  const explicitAttributes = attributes.filter(attr => visualiser.getNode(attr.id));
  explicitAttributes.forEach((attr) => { visualiser.addEdge({ from: nodeId, to: attr.id, label: 'has' }); });
}


// Lazy load attributes and generate label on the nodes that display attributes values
function lazyLoadAttributes(nodes) {
  nodes
    .filter(x => !Array.isArray(x.attributes)) // filter out nodes with empty array ad 'attributes'
    .forEach((node) => {
      EngineClient
        .request({ url: node.attributes })
        .then((resp) => {
          const attributes = Parser.parseAttributes(resp);
          linkNodeToExplicitAttributeNodes(node.id, attributes);
          const label = Visualiser.generateLabel(node.type, attributes, node.label, node.baseType);
          visualiser.updateNode({ id: node.id, attributes, label });
        });
    });
}

function linkRelationshipTypesToRoles(nodes) {
  nodes
    .filter(x => x.baseType === 'RELATIONSHIP_TYPE')
    .forEach((relType) => {
      relType.relates.forEach((roleURI) => {
        EngineClient.request({ url: roleURI })
          .then((resp) => {
            const role = JSON.parse(resp);
            role.roleplayers.forEach((uri) => {
              EngineClient.request({ url: uri })
                .then((roleresp) => {
                  const player = JSON.parse(roleresp);
                  visualiser.addEdge({ from: relType.id, to: player.id, label: role.label });
                });
            });
          });
      });
    });
}

function onGraphResponse(resp: string) {
  const responseObject = JSON.parse(resp);
  const parsedResponse = Parser.parseResponse(responseObject);

  if (!parsedResponse.nodes.length) {
    EventHub.$emit('warning-message', 'No results were found for your query.');
    return;
  }

  // Remove implicit nodes
  const filteredNodes = filterNodes(parsedResponse.nodes);
  // Add filtered nodes to canvas
  filteredNodes.forEach((node) => { visualiser.addNode(node); });
  // Add edges to canvas
  parsedResponse.edges.forEach((edge) => { visualiser.addEdge(edge); });

  // Lazy load attributes once nodes are in the graph
  lazyLoadAttributes(filteredNodes);

  // Load relationship types' roles - this will work only if loading schema concepts i.e. relationship types
  linkRelationshipTypesToRoles(filteredNodes);

  // Get load Role-Players status from local storage
  const rolePlayers = localStorage.getItem('load_role_players');

  // Check if user has set loading of Role-Players
  if(rolePlayers === "true"){

    // If we have 0 edges to display (e.g. when user clicks on Types -> Relationships -> one type of relationship type)
    // force to load relationships role players.
    // This to avoid floating points representing relationships without any roleplayer attached to any of them
    if (parsedResponse.edges.length === 0) {
    filterNodes(parsedResponse.nodes).filter(x => x.baseType.endsWith('RELATIONSHIP'))
      .forEach((rel) => { loadRelationshipRolePlayers(rel); });
    }
  }  
  visualiser.fitGraphToWindow();
}

function fetchFilteredRelationships(href: string) {
  EngineClient.request({
    url: href,
  }).then(resp => onGraphResponse(resp))
    .catch((err) => { EventHub.$emit('error-message', err); });
}

function loadAttributeOwners(attributeId: string) {
  EngineClient.request({
    url: attributeId,
  }).then(resp => onGraphResponse(resp));
}

function getId(str) {
  return str.split('/').pop();
}

function loadRelationshipRolePlayers(rel) {

  // Get Limit set by user on Role-Players shown
  const rolePlayersLimit = localStorage.getItem('role_players_limit');

  const promises = rel.roleplayers.slice(0,rolePlayersLimit).map(x => EngineClient.request({ url: x.thing }));
  Promise.all(promises)
    .then((resps) => { resps.forEach((res) => { onGraphResponse(res); }); })
    .then(() => {
      rel.roleplayers
        .forEach((player) => { visualiser.addEdge({ from: rel.id, to: getId(player.thing), label: getId(player.role) }); });
    });
}

function explainConcept(node) {
  EngineClient.getExplanation(node.explanationQuery)
    .then(onGraphResponse);
}

function loadRelationshipTypeRolePlayers(rel) {
  const promises = rel.relates.map(x => EngineClient.request({ url: x }));
  Promise.all(promises)
    .then((resps) => {
      resps.forEach((res) => {
        // Fetched all roles - dont show them - load all entities that play every role and
        // draw them on canvas
        const role = JSON.parse(res);
        loadRoleRolePlayers(role, rel.id);
      });
    });
}

// load entities that play a given role
function loadRoleRolePlayers(role, relId) {
  role.roleplayers
    .forEach((x) => {
      EngineClient.request({ url: x })
        .then((res) => {
          const entity = JSON.parse(res);
          onGraphResponse(res);
          visualiser.addEdge({ from: relId, to: entity.id, label: role.label });
        });
    });
}

function addAttributeAndEdgeToInstance(instanceId, res) {
  onGraphResponse(res);
  visualiser.addEdge({ from: instanceId, to: JSON.parse(res).id, label: 'has' });
}

function loadEntityRelationships(node) {
  EngineClient.request({ url: node.relationships })
    .then((relationships) => {
      JSON.parse(relationships).relationships.map(rel => EngineClient.request({ url: rel.thing }))
        .forEach((promise) => { promise.then(onGraphResponse); });
    });
}

function showNeighbours(node: Object) {
  const baseType = node.baseType;
  switch (baseType) {
    case 'INFERRED_RELATIONSHIP':
    case 'RELATIONSHIP':
      loadRelationshipRolePlayers(node);
      break;
    case 'ENTITY':
      loadEntityRelationships(node);
      break;
    case 'RELATIONSHIP_TYPE':
      loadRelationshipTypeRolePlayers(node);
      break;
    case 'ENTITY_TYPE':
      // TODO: show nothing as we are not showing roles for now
      break;
    default:
      console.log(`ERROR: Basetype not recognised: ${baseType}`);
      break;
  }
}
function showAttributes(node: Object) {
  node.attributes
    .map(attr => EngineClient.request({ url: attr.href }))
    .forEach((promise) => {
      promise.then(res => addAttributeAndEdgeToInstance(node.id, res));
    });
}


export default {
  initialise, onGraphResponse, fetchFilteredRelationships, loadAttributeOwners, showNeighbours, showAttributes, explainConcept,
};
