/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

import _ from 'underscore';

import * as APITerms from './APITerms';
import * as APIUtils from './APIUtils';

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
        _.map(data, x => { this.parseHalObject(x) });
    }

    parseHalObject(obj) {
        this.newResource(this.getHref(obj), APIUtils.resourceProperties(obj));

        // Add assertions from _embedded
        if(APITerms.KEY_EMBEDDED in obj) {
            _.map(Object.keys(obj[APITerms.KEY_EMBEDDED]), key => {
                this.parseEmbedded(obj[APITerms.KEY_EMBEDDED][key], obj, key)
            });
        }
    }

    /*
    Internal Methods
     */
    /**
     * Parse resources from _embedded field of parent
     */
    parseEmbedded(objs, parent, roleName) {
        _.map(objs, obj => {
            // Add resource and iterate its _embedded field
            var label = APIUtils.relationshipLabel(obj, parent, roleName);
            var hrefA = this.getHref(obj);
            var hrefB = this.getHref(parent);

            this.newResource(hrefA, APIUtils.resourceProperties(obj));

            if(APIUtils.leftSignificant(obj, parent))
                this.newRelationship(hrefA, hrefB, label);
            else
                this.newRelationship(hrefB, hrefA, label);

            this.parseHalObject(obj);

        });
    }

    /**
     * Returns href of current resource.
     * @param resource Object
     * @returns {string|string|*|o}
     */
    getHref(resource) {
        return resource._links.self.href;
    }
}
