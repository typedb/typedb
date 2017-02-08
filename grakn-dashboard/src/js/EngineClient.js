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
import User from './User';

/*
 * REST API client for Grakn Engine.
 */
export default {
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
      type: requestData.requestType || 'GET',
      contentType: requestData.contentType || 'application/json; charset=utf-8',
      dataType: requestData.dataType || 'json',
      cache: requestData.cache || false,
      accepts: requestData.accepts || {
        json: 'application/hal+json',
      },
      data: requestData.data,
      url: requestData.url,
      beforeSend: this.setHeaders,
    }).done((response) => {
      // sometimes we might not have a callback function
      if (typeof requestData.callback === 'function') {
        requestData.callback(response, null);
      }
    })
    .fail((errObj) => {
      if (typeof requestData.callback === 'function') {
        requestData.callback(null, errObj.responseText);
      }
    });
  },

  sendInvite(credentials, callbackFn) {
    $.ajax({
      type: 'POST',
      contentType: 'application/json; charset=utf-8',
      dataType: 'json',
      cache: false,
      data: JSON.stringify({
        name: credentials.name,
        surname: credentials.surname,
        email: credentials.email,
      }),
      url: 'https://grakn-community-inviter.herokuapp.com/invite',
    }).always((r) => {
      callbackFn(r);
    });
  },

  setHeaders(xhr) {
    const token = localStorage.getItem('id_token');
    if (token != null) {
      xhr.setRequestHeader('Authorization', `Bearer ${token}`);
    }

    return true;
  },

  fetchKeyspaces(fn) {
    this.request({
      url: '/keyspaces',
      callback: fn,
      dataType: 'text',
    });
  },

  newSession(creds, fn) {
    this.request({
      url: '/auth/session/',
      callback: fn,
      data: JSON.stringify({
        username: creds.username,
        password: creds.password,
      }),
      dataType: 'text',
      requestType: 'POST',
    });
  },
    /**
     * Pre materialise
     */
  preMaterialiseAll(fn) {
    this.request({
      url: `/graph/preMaterialiseAll?keyspace=${User.getCurrentKeySpace()}`,
      callback: fn,
      dataType: 'text',
      contentType: 'application/text',
    });
  },

    /**
     * Query Engine for concepts by type.
     */
  conceptsByType(type, fn) {
    this.request({
      url: `/graph/concept/${type}?keyspace=${User.getCurrentKeySpace()}`,
      callback: fn,
    });
  },

    /**
     * Send graql shell command to engine. Returns a string representing shell output.
     */
  graqlShell(query, fn) {
    this.request({
      url: `/graph/match?keyspace=${User.getCurrentKeySpace()}&query=${encodeURIComponent(query)}&reasoner=${User.getReasonerStatus()}&materialise=${User.getMaterialiseStatus()}`,
      callback: fn,
      dataType: 'text',
      contentType: 'application/text',
      accepts: {
        text: 'application/graql',
      },
    });
  },

    /**
     * Send graql query to Engine, returns an array of HAL objects.
     */
  graqlHAL(query, fn) {
    this.request({
      url: `/graph/match?keyspace=${User.getCurrentKeySpace()}&query=${encodeURIComponent(query)}&reasoner=${User.getReasonerStatus()}&materialise=${User.getMaterialiseStatus()}`,
      callback: fn,
    });
  },

    /**
     * Send graql query to Engine, returns an array of HAL objects.
     */
  graqlAnalytics(query, fn) {
    this.request({
      url: `/graph/analytics?keyspace=${User.getCurrentKeySpace()}&query=${encodeURIComponent(query)}`,
      callback: fn,
    });
  },
    /**
     * Get current engine configuration.
     */
  getConfig(fn) {
    this.request({
      url: '/status/config',
      callback: fn,
    });
  },

    /**
     * Get meta ontology type instances.
     */
  getMetaTypes(fn) {
    this.request({
      url: `/graph/ontology?keyspace=${User.getCurrentKeySpace()}`,
      callback: fn,
    });
  },
};
