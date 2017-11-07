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

import * as API from '../util/HALTerms';
import * as Utils from './APIUtils';
import _ from 'underscore';


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

const metaTypesSet = {
  ENTITY_TYPE: true,
  ATTRIBUTE_TYPE: true,
  ROLE_TYPE: true,
  RELATIONSHIP_TYPE: true,
  RULE_TYPE: true,
};


   /**
    * Parse HAL object to extract default properties, attributes and links.
    * Add new node object to the nodes array that will be returned to invoker of HALParser.
    * @param {*} nodeObj HAL object that will be turned into graph node
    * @param {*} nodes array containing the resulting set of graph nodes
    * @private
    */

function newNode(nodeObj:Object, nodes:Object[]) {
  const links = Utils.nodeLinks(nodeObj);
  const properties = Utils.defaultProperties(nodeObj);
  const attributes = Utils.extractAttributes(nodeObj);
  nodes.push({ properties, attributes, links });
}


  /**
   * Add a new edge to the edges array that will be returned to the invoker of HALParser.
   * @param {*} parent HAL object in which child is embedded
   * @param {*} child  HAL object embedded in parent object that is connected to it
   * @param {*} roleName label describing relationship between parent and child
   * @param {*} edges array containing the resulting set of graph edges
   * @private
   */
function newEdge(parent:Object, child:Object, roleName:string, edges:Object[]) {
  const idC = child[API.KEY_ID];
  const idP = parent[API.KEY_ID];
  const edgeLabel = (roleName === API.KEY_EMPTY_ROLE_NAME) ? '' : roleName;

  if (Utils.edgeLeftToRight(parent, child)) {
    edges.push({ from: idC, to: idP, label: edgeLabel });
  } else {
    edges.push({ from: idP, to: idC, label: edgeLabel });
  }
}

   /**
    * Given a set of embedded HAL objects parse them into graph nodes, recursively
    * @param {*} objs HAL objects that need to be parsed into graph nodes
    * @param {*} parent parent HAL object in which objs are embedded
    * @param {*} roleName label describing relationship between parent and objects in objs
    * @param {*} showIsa boolean used to determine whether we should parse "isa" embedded objects
    * @param {*} nodes array containing the resulting set of graph nodes
    * @param {*} edges array containing the resulting set of graph edges
    * @private
    */
function parseEmbedded(objs:Object[], parent:Object, roleName:string, showIsa:boolean, nodes:Object[], edges:Object[]) {
  objs.forEach((child) => {
    newEdge(parent, child, roleName, edges);
    parseHalObject(child, showIsa, nodes, edges);
  });
}


  /**
   * Parse single HAL object and its embedded into graph nodes
   * @param {Object} obj HAL object that needs to be parsed into graph node
   * @param {boolean} showIsa boolean used to determine whether we should parse "isa" embedded objects
   * @param {Object[]} nodes array containing the resulting set of graph nodes
   * @param {Object[]} edges array containing the resulting set of graph edges
   * @private
   */
function parseHalObject(obj:Object, showIsa:boolean, nodes:Object[], edges:Object[]) {
  newNode(obj, nodes);

  if (API.KEY_EMBEDDED in obj) {
    Object.keys(obj[API.KEY_EMBEDDED]).forEach((key) => {
      const embeddedNodes = obj[API.KEY_EMBEDDED][key];
        // We can safely check only element 0 since we only support single inheritance.
      if ((key !== 'isa') || showIsa === true || obj._baseType in metaTypesSet) {
        parseEmbedded(obj[API.KEY_EMBEDDED][key], obj, key, showIsa, nodes, edges);
      }
    });
  }
}

function flat(array){
  return array.flatMap(x => Object.values(x).reduce((array, current) => array.concat(current), []));
}

export default {
   /**
    * Given a JSON object/array in HAL format returns a set of graph nodes and edges
    * @param {Object|Object[]} data HAL object/array
    * @param {boolean} showIsa boolean used to determine whether we should parse "isa" embedded objects
    * @returns {Object} Object containing two arrays containing graph nodes and edges
    * @public
    */
  parseResponse(data: Object|Object[], showIsa:boolean) {
    const nodes = [];
    const edges = [];

    try {
      let dataArray = (Array.isArray(data)) ? flat(data) : [data];

      dataArray
      .forEach((x) => {
        parseHalObject(x, showIsa, nodes, edges);
      });
    } catch (error) {
      console.log(`GRAKN Exception while parsing HAL response: \n ${error.stack}`);
    }
    const uniqueNodes = _.map(_.groupBy(nodes, doc => doc.properties.id), grouped => grouped[0]);
    return { nodes: uniqueNodes, edges };
  },
};
