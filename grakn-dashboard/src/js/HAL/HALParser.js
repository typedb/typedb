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

import _ from 'underscore';

import * as API from '../util/HALTerms';
import * as Utils from './APIUtils';

/*
 * Parses HAL responses with callbacks (for found HAL resources & relationships).
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

export default class HALParser {

  constructor() {
    this.newResource = () => {};
    this.newRelationship = () => {};
    this.nodeAlreadyInGraph = () => {};

    this.metaTypesSet = {
      ENTITY_TYPE: true,
      RESOURCE_TYPE: true,
      ROLE_TYPE: true,
      RELATION_TYPE: true,
      RULE_TYPE: true,
    };
  }

    /**
     * Callback for processing a resource. @fn(id, properties)
     */
  setNewResource(fn) {
    this.newResource = fn;
  }

  setNodeAlreadyInGraph(fn) {
    this.nodeAlreadyInGraph = fn;
  }

    /**
     * Callback for processing a relationship between two resources. @fn(from, to, label)
     */
  setNewRelationship(fn) {
    this.newRelationship = fn;
  }

    /**
     * Start parsing HAL response in @data.
     * Will call functions set by setNewResource() and setNewRelationship().
     */
  parseResponse(data, showIsa) {
    if (Array.isArray(data)) {
      const hashSet = {};
      const objLength = data.length;
            // Populate hashSet containing all the first level objects returned in the response, they MUST be added to the graph.
      for (let i = 0; i < objLength; i++) {
        hashSet[data[i]._id] = true;
      }
      _.map(data, (x) => {
        this.parseHalObject(x, hashSet, showIsa);
      });
      return data.length;
    }

    this.parseHalObject(data, {}, showIsa);
    return 1;
  }


  parseHalObject(obj, hashSet, showIsa) {
    if (obj !== null) {
            // The response from Analytics will be a string instead of object. That's why we need this check.
            // we need this because when we loop through embedded we want to draw the edge that points to all the first order nodes.
      const objResponse = (typeof obj === 'string') ? JSON.parse(obj) : obj;

      const links = Utils.nodeLinks(objResponse);

      this.newResource(HALParser.getHref(objResponse), Utils.defaultProperties(objResponse), Utils.extractResources(objResponse), links);
            // Add assertions from _embedded
      if (API.KEY_EMBEDDED in objResponse) {
        _.map(Object.keys(objResponse[API.KEY_EMBEDDED]), (key) => {
          if ((key !== 'isa') || showIsa === true || objResponse._baseType in this.metaTypesSet) {
            this.parseEmbedded(objResponse[API.KEY_EMBEDDED][key], objResponse, key, hashSet);
          }
        });
      }
    }
  }

    /*
    Internal Methods
     */
    /**
     * Parse resources from _embedded field of parent
     */
  parseEmbedded(objs, parent, roleName, hashSet) {
    _.map(objs, (child) => {
            // Add embedded object to the graph only if one of the following is satisfied:
            // - the current node is not a RESOURCE_TYPE
            // - the current node is already drawn in the graph
            // - the current node is contained in the response as first level object (not embdedded)
            //    if it's contained in the hashset it means it MUST be draw and so all the adges pointing to it.

      if (((child[API.KEY_BASE_TYPE] !== API.RESOURCE_TYPE) && (child[API.KEY_BASE_TYPE] !== API.RESOURCE)) ||
                (hashSet !== undefined && hashSet[child._id]) ||
                this.nodeAlreadyInGraph(HALParser.getHref(child))) {
        const links = Utils.nodeLinks(child);
                // Add resource and iterate its _embedded field
        const idC = child[API.KEY_ID];
        const idP = parent[API.KEY_ID];

        this.newResource(HALParser.getHref(child),
                    Utils.defaultProperties(child),
                    Utils.extractResources(child), links);

        const edgeLabel = (roleName === API.KEY_EMPTY_ROLE_NAME) ? '' : roleName;

        if (Utils.edgeLeftToRight(parent, child)) {
          this.newRelationship(idC, idP, edgeLabel);
        } else {
          this.newRelationship(idP, idC, edgeLabel);
        }

        this.parseHalObject(child);
      }
    });
  }


    /**
     * Returns href of current resource.
     * @param resource Object
     * @returns {string|string|*|o}
     */
  static getHref(resource) {
    return resource[API.KEY_LINKS][API.KEY_SELF][API.KEY_HREF];
  }
}
