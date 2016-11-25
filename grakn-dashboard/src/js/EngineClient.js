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

"use strict";

/*
 * REST API client for Grakn Engine.
 */
export default class EngineClient {
    constructor() {
        this.requestType = 'GET';
        this.contentType = 'application/json; charset=utf-8';
        this.dataType = 'json';
        this.cache = false;
    }

    // can use queue of pending requests here..

    /**
     * Make an AJAX request with @requestData parameters.
     * Required attributes of @requestData are:
     *  - url
     *  - callback
     * Optional attributes:
     *  - data
     * Optional attributes with defaults:
     *  - type
     *  - contentType
     *  - dataType
     *  - cache
     * @param requestData
     */
    request(requestData) {
        $.ajax({
                type: requestData.requestType || this.requestType,
                contentType: requestData.contentType || this.contentType,
                dataType: requestData.dataType || this.dataType,
                cache: requestData.cache || this.cache,
                data: requestData.data,
                url: requestData.url
            }).done(function(r) {
                //sometimes we might not have a callback function
                if(typeof requestData.callback == 'function')
                    requestData.callback(r, null)
            })
            .fail(function(errObj) {
                if(typeof requestData.callback == 'function')
                    requestData.callback(null, errObj.responseText);
            });
    }

    /**
     * Query Engine for concepts by type.
     */
    conceptsByType(type, fn) {
        this.request({
            url: "/graph/concept/" + type,
            callback: fn
        });
    }

    /**
     * Send graql shell command to engine. Returns a string representing shell output.
     */
    graqlShell(query, fn) {
        this.request({
            url: "/shell/match?query=" + query,
            callback: fn,
            dataType: "text",
            contentType: "application/text"
        });
    }

    /**
     * Pre materialise
     */
    preMaterialiseAll(fn) {
        this.request({
            url: "/graph/preMaterialiseAll",
            callback: fn,
            dataType: "text",
            contentType: "application/text"
        });
    }

    /**
     * Send graql query to Engine, returns an array of HAL objects.
     */
    graqlHAL(query, useReasoner, fn) {
        this.request({
            url: "/graph/match?query=" + query + "&reasoner=" + useReasoner,
            callback: fn
        });
    }

    /**
     * Send graql query to Engine, returns an array of HAL objects.
     */
    graqlAnalytics(query, fn) {
        this.request({
            url: "/graph/analytics?query=" + query,
            callback: fn
        });
    }

    /**
     * Get current engine configuration.
     */
    getConfig(fn) {
        this.request({
            url: "/status/config",
            callback: fn
        });
    }

    /**
     * Get meta ontology type instances.
     */
    getMetaTypes(fn) {
        this.request({
            url: "/shell/metaTypeInstances",
            callback: fn
        });
    }
};
