/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

const grpc = require("grpc");
const messages = require("../../../client-nodejs-proto/protocol/keyspace/Keyspace_pb");
const service = require("../../../client-nodejs-proto/protocol/keyspace/Keyspace_grpc_pb");

function KeyspaceService(uri, credentials) {
    this.uri = uri;
    this.credentials = credentials;
    this.stub = new service.KeyspaceServiceClient(uri, grpc.credentials.createInsecure());
}


KeyspaceService.prototype.retrieve = function () {
    const retrieveRequest = new messages.Keyspace.Retrieve.Req();
    return new Promise((resolve, reject) => {
        this.stub.retrieve(retrieveRequest, (err, resp) => {
            if (err) reject(err);
            else resolve(resp.getNamesList());
        });
    })
}

KeyspaceService.prototype.delete = function (keyspace) {
    const deleteRequest = new messages.Keyspace.Delete.Req();
    deleteRequest.setName(keyspace);
    return new Promise((resolve, reject) => {
        this.stub.delete(deleteRequest, (err) => {
            if (err) reject(err);
            else resolve();
        });
    });
}

KeyspaceService.prototype.close = function close() {
    grpc.closeClient(this.stub);
}

module.exports = KeyspaceService;