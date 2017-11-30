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
import * as Utils from '../../../js/Parser/APIUtils';
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
    .then((resp, nodeId) => onGraphResponse(resp, false, false, false, nodeId))
    .catch((err) => { EventHub.$emit('error-message', err); });
  }
}

function onLoadSchema(type: string) {
  const querySub = `match $x sub ${type}; get;`;
  EngineClient.graqlQuery(querySub)
  .then(resp => onGraphResponse(resp, false, false, false))
  .catch((err) => { EventHub.$emit('error-message', err); });
}

function onGraphResponseAnalytics(resp: string) {
  const responseObject = JSON.parse(resp);
  EventHub.$emit('analytics-string-response', responseObject);
}

function flatten<T>(array: T[][]): T[] {
  return array.reduce((x, y) => x.concat(y), []);
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

function onGraphResponse(resp: string, showIsa: boolean, showAttributes: boolean, isExplore: boolean, nodeId:?string) {
  const responseObject = JSON.parse(resp);
  const parsedResponse = Parser.parseResponse(responseObject);

  if (!parsedResponse.nodes.length) {
    EventHub.$emit('warning-message', 'No results were found for your query.');
    return;
  }

  parsedResponse.nodes
  // .filter(x => !x.implicit)
  // .filter(x => x.baseType !== 'ATTRIBUTE_TYPE')
  .forEach(node => visualiser.addNode(node, node.attributes, node.links, nodeId));
  parsedResponse.edges.forEach(edge => visualiser.addEdge(edge.from, edge.to, edge.label));

  visualiser.fitGraphToWindow();
}

function fetchFilteredRelationships(href: string) {
  EngineClient.request({
    url: href,
  }).then(resp => onGraphResponse(resp, false, false, false))
  .catch((err) => { EventHub.$emit('error-message', err); });
}

function loadAttributeOwners(attributeId: string) {
  EngineClient.request({
    url: attributeId,
  }).then(resp => onGraphResponse(resp, false, true, false));
}


export default { initialise, onGraphResponse, fetchFilteredRelationships, loadAttributeOwners };
