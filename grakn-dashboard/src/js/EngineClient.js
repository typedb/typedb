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
import User from './User';
/*
 * REST API client for Grakn Engine.
 */
export default {
  /**
     * Make an AJAX request with @requestData parameters.
     * Required attributes of @requestData are:
     *  - url
     * Optional attributes with defaults:
     *  - cache
     * @param requestData
     */
  request(requestData:Object) {
    return new Promise((resolve, reject) => {
      try {
        const req = new XMLHttpRequest();
        req.open(requestData.requestType || 'GET', requestData.url);
        this.setHeaders(req, requestData);

        req.onload = function setOnLoad() {
          if (req.status === 200) {
            resolve(req.response);
          } else {
            reject(Error(req.response));
          }
        };

        // Handle network errors
        req.onerror = function setOnError() {
          reject(Error('Network Error'));
        };

        // Make the request
        req.send(requestData.data);
      } catch (exception) {
        reject(exception);
      }
    });
  },

  setHeaders(xhr:Object, requestData:Object) {
    xhr.setRequestHeader('Content-Type', requestData.contentType || 'application/json; charset=utf-8');
    if (requestData.accepts) xhr.setRequestHeader('Accept', requestData.accepts);
  },

  sendInvite(credentials:Object, callbackFn:()=>mixed) {
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

  fetchKeyspaces() {
    return this.request({
      url: '/kb',
    });
  },

  newSession(creds:Object) {
    return this.request({
      url: '/auth/session/',
      data: JSON.stringify({
        username: creds.username,
        password: creds.password,
      }),
      requestType: 'POST',
    });
  },

  /**
             * Send graql shell command to engine. Returns a string representing shell output.
             */
  graqlShell(query:string) {
    return this.request({
      url: `/kb/${User.getCurrentKeySpace()}/graql?infer=${User.getReasonerStatus()}`,
      contentType: 'application/text',
      accepts: 'application/text',
      requestType: 'POST',
      data: query,
    });
  },
  /**
             * Send graql query to Engine, returns an array of HAL objects.
             */
  graqlQuery(query:string) {
    // In get queries we are also attaching a limit for the embedded objects of the resulting nodes, this is not the query limit.
    return this.request({
      url: `/kb/${User.getCurrentKeySpace()}/graql?infer=${User.getReasonerStatus()}&defineAllVars=true`,
      requestType: 'POST',
      data: query,
    });
  },
  /**
             * Send graql query to Engine, returns an array of HAL objects.
             */
  graqlAnalytics(query:string) {
    return this.request({
      url: `/kb/${User.getCurrentKeySpace()}/graql?infer=${User.getReasonerStatus()}`,
      requestType: 'POST',
      accepts: 'application/text',
      data: query,
    });
  },
  /**
   * Get current engine configuration.
   */
  getConfig() {
    return this.request({
      url: `/kb/${User.getCurrentKeySpace()}`,
      requestType: 'PUT',
    });
  },

  getVersion() {
    return this.request({
      url: '/version',
    });
  },
  /**
             * Get meta schema type instances.
             */
  getMetaTypes() {
    return this.request({
      url: `/kb/${User.getCurrentKeySpace()}/type`,
      accepts: 'application/json',
    });
  },

  getConceptTypes(id:string) {
    return this.request({
      url: `/dashboard/types/${id}?keyspace=${User.getCurrentKeySpace()}`,
      accepts: 'application/json',
    });
  },
};
