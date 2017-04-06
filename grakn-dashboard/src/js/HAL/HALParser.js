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
import EngineClient from '../EngineClient';


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

    this.instances = [];
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
  parseResponse(data, showIsa, showResources, nodeId) {
    let responseLength;

    if (Array.isArray(data)) {
      const hashSet = {};
      const objLength = data.length;

      // Populate hashSet containing all the first level objects returned in the response, they MUST be added to the graph.
      for (let i = 0; i < objLength; i++) {
        hashSet[data[i]._id] = true;
      }

      data.forEach((x) => {
        this.parseHalObject(x, hashSet, showIsa, showResources, nodeId);
      });

      responseLength = data.length;
    } else {
      this.parseHalObject(data, {}, showIsa, showResources, nodeId);
      responseLength = 1;
    }

    // Load all the resources of the new instances added to the graph
    this.loadInstancesResources(0, this.instances);
    this.emptyInstances();

    return responseLength;
  }

  loadInstancesResources(start, instances) {
    const batchSize = 50;
    const promises = [];

    // Add a batchSize number of requests to the promises array
    for (let i = start; i < start + batchSize; i++) {
      if (i >= instances.length) {
        // When all the requests are loaded in promises flush the remaining ones and update labels on nodes
        this.flushPromisesAndRefreshLabels(promises, instances);
        return;
      }
      promises.push(EngineClient.request({
        url: instances[i].href,
      }));
    }

    // Execute all the promises and once they are all done recursively call this function again
    Promise.all(promises).then((responses) => {
      responses.forEach((resp) => {
        const respObj = JSON.parse(resp).response;
        // Check if some of the resources attached to this node are already drawn in the graph:
        // if a resource is already in the graph (because explicitly asked for (e.g. all relations with weight > 0.5 ))
        // we need to draw the edges connecting this node to the resource node.
        if (API.KEY_EMBEDDED in respObj) {
          Object.keys(respObj[API.KEY_EMBEDDED]).forEach((key) => {
            if ((key !== 'isa') || (respObj._baseType === API.RESOURCE)) {
              this.parseEmbedded(respObj[API.KEY_EMBEDDED][key], respObj, key, {}, false, false);
            }
          });
        }
        visualiser.updateNodeResources(respObj[API.KEY_ID], Utils.extractResources(respObj));
      });
      visualiser.flushUpdates();
      this.loadInstancesResources(start + batchSize, instances);
    });
  }

  flushPromisesAndRefreshLabels(promises, instances) {
    Promise.all(promises).then((responses) => {
      responses.forEach((resp) => {
        const respObj = JSON.parse(resp).response;

        // Check if some of the resources attached to this node are already drawn in the graph:
        // if a resource is already in the graph (because explicitly asked for (e.g. all relations with weight > 0.5 ))
        // we need to draw the edges connecting this node to the resource node.
        if (API.KEY_EMBEDDED in respObj) {
           Object.keys(respObj[API.KEY_EMBEDDED]).forEach((key) => {
            if ((key !== 'isa') || (respObj._baseType === API.RESOURCE)) {
              this.parseEmbedded(respObj[API.KEY_EMBEDDED][key], respObj, key, {}, false, false);
            }
          });
         }
        visualiser.updateNodeResources(respObj[API.KEY_ID], Utils.extractResources(respObj));
      });
      visualiser.flushUpdates();
      visualiser.refreshLabels(instances);
    });
  }
  parseHalObject(obj, hashSet, showIsa, showResources, nodeId) {
    if (obj !== null) {
            // The response from Analytics will be a string instead of object. That's why we need this check.
            // we need this because when we loop through embedded we want to draw the edge that points to all the first order nodes.
      const objResponse = (typeof obj === 'string') ? JSON.parse(obj) : obj;

      this.newNode(objResponse, nodeId);

            // Add assertions from _embedded
      if (API.KEY_EMBEDDED in objResponse) {
        _.map(Object.keys(objResponse[API.KEY_EMBEDDED]), (key) => {
          if ((key !== 'isa') || showIsa === true || objResponse._baseType in this.metaTypesSet) {
            this.parseEmbedded(objResponse[API.KEY_EMBEDDED][key], objResponse, key, hashSet, showIsa, showResources, nodeId);
          }
        });
      }
    }
  }

  emptyInstances() {
    this.instances = [];
  }
    /*
    Internal Methods
     */
    /**
     * Parse resources from _embedded field of parent
     */
  parseEmbedded(objs, parent, roleName, hashSet, showIsa, showResources, nodeId) {
    objs.forEach((child) => {
            // Add embedded object to the graph only if one of the following is satisfied:
            // - the current node is not a RESOURCE_TYPE || showResources is set to true
            // - the current node is already drawn in the graph
            // - the current node is contained in the response as first level object (not embdedded)
            //    if it's contained in the hashset it means it MUST be drawn and so all the edges pointing to it.

      if (((child[API.KEY_BASE_TYPE] !== API.RESOURCE_TYPE)
          && (child[API.KEY_BASE_TYPE] !== API.RESOURCE)
          || showResources)
          || (hashSet !== undefined && hashSet[child._id])
          || this.nodeAlreadyInGraph(child._id)) {
                // Add resource and iterate its _embedded field
        const idC = child[API.KEY_ID];
        const idP = parent[API.KEY_ID];

        this.newNode(child, nodeId);

        const edgeLabel = (roleName === API.KEY_EMPTY_ROLE_NAME) ? '' : roleName;

        if (Utils.edgeLeftToRight(parent, child)) {
          this.newRelationship(idC, idP, edgeLabel);
        } else {
          this.newRelationship(idP, idC, edgeLabel);
        }

        this.parseHalObject(child, hashSet, showIsa, showResources, nodeId);
      }
    });
  }

  newNode(nodeObj, nodeId) {
    const links = Utils.nodeLinks(nodeObj);
  // Load instance resouces if the node is not already in the graph
    if (nodeObj[API.KEY_BASE_TYPE] === API.ENTITY || nodeObj[API.KEY_BASE_TYPE] === API.RELATION || nodeObj[API.KEY_BASE_TYPE] === API.RULE) {
      if (!visualiser.nodeExists(nodeObj[API.KEY_ID])) {
        this.instances.push({ id: nodeObj[API.KEY_ID], href: nodeObj[API.KEY_LINKS][API.KEY_EXPLORE][0][API.KEY_HREF] });
      }
    }
    this.newResource(HALParser.getHref(nodeObj),
              Utils.defaultProperties(nodeObj),
              Utils.extractResources(nodeObj), links, nodeId);
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
