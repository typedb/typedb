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

import * as API from './APITerms';
import * as Utils from './APIUtils';

/*
 * Parses HAL responses with callbacks (for found HAL resources & relationships).
 */
export const URL_REGEX = "^(?:(?:https?|ftp)://)(?:\\S+(?::\\S*)?@)?(?:" +
    "(?!(?:10|127)(?:\\.\\d{1,3}){3})" +
    "(?!(?:169\\.254|192\\.168)(?:\\.\\d{1,3}){2})" +
    "(?!172\\.(?:1[6-9]|2\\d|3[0-1])(?:\\.\\d{1,3}){2})" +
    "(?:[1-9]\\d?|1\\d\\d|2[01]\\d|22[0-3])" +
    "(?:\\.(?:1?\\d{1,2}|2[0-4]\\d|25[0-5])){2}" +
    "(?:\\.(?:[1-9]\\d?|1\\d\\d|2[0-4]\\d|25[0-4]))" +
    "|(?:(?:[a-z\\u00a1-\\uffff0-9]-*)*[a-z\\u00a1-\\uffff0-9]+)" +
    "(?:\\.(?:[a-z\\u00a1-\\uffff0-9]-*)*[a-z\\u00a1-\\uffff0-9]+)*" +
    "(?:\\.(?:[a-z\\u00a1-\\uffff]{2,}))\\.?)(?::\\d{2,5})?" +
    "(?:[/?#]\\S*)?$";

export default class HALParser {

    constructor() {
        this.newResource = x => {};
        this.newRelationship = x => {};
        this.nodeAlreadyInGraph = x => {};
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
     * Start parsing HAL response in @data. Will call functions set by setNewResource() and setNewRelationship().
     */
    parseResponse(data) {
        if (Array.isArray(data)) {
            _.map(data, x => {
                this.parseHalObject(x)
            });
            return data.length;
        } else {
            this.parseHalObject(data);
            return 1;
        }
    }

    parseHalObject(obj) {
        if (obj !== null) {
            let objResponse;
            //The response from Analytics will be a string instead of object. That's why we need this check.
            objResponse = (typeof obj === 'string') ? JSON.parse(obj) : obj;

            let links = Utils.nodeLinks(objResponse);

            this.newResource(this.getHref(objResponse), Utils.defaultProperties(objResponse), Utils.extractResources(objResponse), links);
            // Add assertions from _embedded
            if (API.KEY_EMBEDDED in objResponse) {
                _.map(Object.keys(objResponse[API.KEY_EMBEDDED]), key => {
                    this.parseEmbedded(objResponse[API.KEY_EMBEDDED][key], objResponse, key)
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
    parseEmbedded(objs, parent, roleName) {
        _.map(objs, child => {
            if ((child[API.KEY_BASE_TYPE] != API.RESOURCE_TYPE) || this.nodeAlreadyInGraph(this.getHref(child))) {
                var links = Utils.nodeLinks(child);
                // Add resource and iterate its _embedded field
                var hrefP = this.getHref(child);
                var hrefC = this.getHref(parent);

                this.newResource(hrefP, Utils.defaultProperties(child), Utils.extractResources(child), links);

                let edgeLabel = (roleName === API.KEY_EMPTY_ROLE_NAME) ? "" : roleName;

                if (Utils.edgeLeftToRight(parent, child))
                    this.newRelationship(hrefP, hrefC, edgeLabel);
                else
                    this.newRelationship(hrefC, hrefP, edgeLabel);

                this.parseHalObject(child);
            }
        });
    }


    /**
     * Returns href of current resource.
     * @param resource Object
     * @returns {string|string|*|o}
     */
    getHref(resource) {
        return resource[API.KEY_LINKS][API.KEY_SELF][API.KEY_HREF];
    }
}
