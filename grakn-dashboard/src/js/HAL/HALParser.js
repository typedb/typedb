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
export default class HALParser {
    constructor() {
        this.newResource = x => {};
        this.newRelationship = x => {};
    }

    /**
     * Callback for processing a resource. @fn(id, properties)
     */
    setNewResource(fn) {
        this.newResource = fn;
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
        _.map(data, x => {
            this.parseHalObject(x)
        });
        return data.length;
    }

    parseHalObject(obj) {
        if (obj !== null) {
            var links = Utils.nodeLinks(obj);
            this.newResource(this.getHref(obj), Utils.defaultProperties(obj), Utils.extractResources(obj), links);
            // Add assertions from _embedded
            if (API.KEY_EMBEDDED in obj) {
                _.map(Object.keys(obj[API.KEY_EMBEDDED]), key => {
                    this.parseEmbedded(obj[API.KEY_EMBEDDED][key], obj, key)
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
            if (child[API.KEY_BASE_TYPE] != API.RESOURCE_TYPE) {
                var links = Utils.nodeLinks(child);
                // Add resource and iterate its _embedded field
                var hrefP = this.getHref(child);
                var hrefC = this.getHref(parent);

                this.newResource(hrefP, Utils.defaultProperties(child), Utils.extractResources(child), links);

                if (Utils.edgeLeftToRight(parent, child))
                    this.newRelationship(hrefP, hrefC, roleName);
                else
                    this.newRelationship(hrefC, hrefP, roleName);

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
