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

import { KEY_BASE_TYPE, RELATIONSHIP_TYPE, RELATIONSHIP, AT_ID, KEY_ID, conceptProperties, parseAttributes } from './APIUtils';


/**
 * Regular expression used to match URIs contained in attributes values
 */
export const URL_REGEX = '^(?:(?:https?|ftp)://)(?:\\S+(?::\\S*)?@)?(?:' +
    '(?!(?:10|127)(?:\\.\\d{1,3}){3})' +
    '(?!(?:169\\.254|192\\.168)(?:\\.\\d{1,3}){2})' +
    '(?!172\\.(?:1[6-9]|2\\d|3[0-1])(?:\\.\\d{1,3}){2})' +
    '(?:[1-9]\\d?|1\\d\\d|2[01]\\d|22[0-3])' +
    '(?:\\.(?:1?\\d{1,2}|2[0-4]\\d|25[0-5])){2}' +
    '(?:\\.(?:[1-9]\\d?|1\\d\\d|2[0-4]\\d|25[0-4]))' +
    '|(?:(?:[a-z\\u00a1-\\uffff0-9]-*)*[a-z\\u00a1-\\uffff0-9]+)' +
    '(?:\\.(?:[a-z\\u00a1-\\uffff0-9]-*)*[a-z\\u00a1-\\uffff0-9]+)*' +
    '(?:\\.(?:[a-z\\u00a1-\\uffff]{2,}))\\.?)(?::\\d{2,5})?' +
    '(?:[/?#]\\S*)?$';

const collect = (array, current) => array.concat(current);
function flat(array) {
  return array.flatMap(x => Object.values(x).reduce((array, current) => array.concat(current), []));
}

function newNode(nodeObj:Object) {
  const properties = conceptProperties(nodeObj);
  // TODO: decide whether list attributes also on meta type node for now we just set empty array
  const attributes = (('label' in nodeObj) || nodeObj.inferred) ? [] : nodeObj.attributes;
  const relationships = nodeObj.relationships || [];
  const roleplayers = nodeObj.roleplayers || [];
  const relates = nodeObj.relates || [];
  return Object.assign({}, properties, {
    attributes, relationships, roleplayers, relates,
  });
}

function populateRolesMap(nodes) {
  const rolesMap = {};
  nodes.forEach((node) => {
    node.plays.forEach((roleUri) => {
      rolesMap[roleUri] = ((rolesMap[roleUri]) ? rolesMap[roleUri] : []).concat(node.id);
    });
  });
  return rolesMap;
}

function populateInstancesMap(nodes) {
  return nodes
    .map(node => ({ [node[AT_ID]]: node[KEY_ID] }))
    .reduce((map, current) => (Object.assign(map, current)), {});
}

function relationshipEdges(relationObj, instancesMap) {
  return relationObj.roleplayers
    .filter(player => (player.thing in instancesMap))
    .map(player => ({ from: relationObj.id, to: instancesMap[player.thing], label: player.role.split('/').pop() }))
    .reduce(collect, []);
}

function relationshipTypeEdges(relationObj, rolesMap) {
  return relationObj.relates
    .filter(uri => (uri in rolesMap))
    .flatMap(uri => rolesMap[uri].map(to => ({ from: relationObj.id, to, label: uri.split('/').pop() })))
    .reduce(collect, []);
}


export default {
  /**
    * Given a JSON object/array in HAL format returns a set of graph nodes and edges
    * @param {Object|Object[]} data HAL object/array
    * @returns {Object} Object containing two arrays containing graph nodes and edges
    * @public
    */
  parseResponse(data: Object|Object[]) {
    let dataArray;

    if (Array.isArray(data)) {
      if ((data.length > 0) && ('@id' in data[0])) {
        dataArray = data;
      } else {
        dataArray = flat(data);
      }
    } else {
      dataArray = [data];
    }

    // COMPUTE NODES
    const nodes = dataArray.map(x => newNode(x)).reduce(collect, []);

    // COMPUTE EDGES IN SCHEMA: Relationship types have roles in the field 'relates'

    // ( Helper map {roleURI:[nodeId..]} )
    const rolesMap = populateRolesMap(dataArray.filter(node => node.plays));
    const schemaEdges = dataArray
      .filter(x => x[KEY_BASE_TYPE] === RELATIONSHIP_TYPE)
      .map(x => relationshipTypeEdges(x, rolesMap))
      .reduce(collect, []);

    // COMPUTE EDGES IN INSTANCES: Relationship instances have roles in the field 'roleplayers'

    // ( Helper map {nodeURI: nodeId} )
    const instancesMap = populateInstancesMap(dataArray);
    const instanceEdges = dataArray
      .filter(x => x[KEY_BASE_TYPE] === RELATIONSHIP)
      .map(x => relationshipEdges(x, instancesMap))
      .reduce(collect, []);

    return { nodes, edges: schemaEdges.concat(instanceEdges) };
  },
  parseAttributes(data) { return parseAttributes(JSON.parse(data)); },
};
