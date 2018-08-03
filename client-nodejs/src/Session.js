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

const Transaction = require("./Transaction");
const SessionService = require("./service/Session/SessionService");


/**
 * Session object that can be used to:
 *  - create a new Transaction
 * 
 * @param {String} uri String containing host and port of a valid Grakn server 
 * @param {String} keyspace Grakn keyspace to which this sessions should be bound to
 * @param {Object} credentials Optional object containing user credentials - only used when connecting to a KGMS instance
 */
function Session(uri, keyspace, credentials) {
  this.sessionService = new SessionService(uri, keyspace, credentials);
}

/**
 * Create new Transaction, which is already open and ready to be used.
 * @param {Grakn.txType} txType Type of transaction to open READ, WRITE or BATCH
 * @returns {Transaction}
 */
Session.prototype.transaction = async function (txType) {
  const transactionService = await this.sessionService.transaction(txType).catch(e => { throw e; });
  return new Transaction(transactionService);
}

/**
 * Close stream connected to gRPC server
 */
Session.prototype.close = function close() {
  this.sessionService.close();
}

module.exports = Session;
