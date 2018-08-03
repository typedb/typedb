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

/**
 * This object represents a transaction which is produced by Grakn and allows the user to construct and perform
 * basic look ups to the knowledge base. This also allows the execution of Graql queries.
 * 
 * @param {Object} txService Object implementing all the functionalities of gRPC Transaction service as defined in grakn.proto
 */
function Transaction(txService) {
    this.txService = txService;
}

/**
 * Executes a given Graql query on the keyspace this transaction is bound to
 * @param {String} query String representing a Graql query 
 */
Transaction.prototype.query = function executeQuery(query, options) {
    return this.txService.query(query, options);
};

/**
 * Commits any changes to the graph and closes the transaction. The user must use the Session object to
 * get a new open transaction.
 */
Transaction.prototype.commit = async function () {
    await this.txService.commit();
    return this.close();
}

/**
 * Get the Concept with identifier provided, if it exists.
 * 
 * @param {String} conceptId A unique identifier for the Concept in the graph.
 */
Transaction.prototype.getConcept = function (conceptId) {
    return this.txService.getConcept(conceptId);
}

Transaction.prototype.getSchemaConcept = function (label) {
    return this.txService.getSchemaConcept(label);
}

Transaction.prototype.getAttributesByValue = function (attributeValue, dataType) {
    return this.txService.getAttributesByValue(attributeValue, dataType);
}

Transaction.prototype.putEntityType = function (label) {
    return this.txService.putEntityType(label);
}

Transaction.prototype.putRelationshipType = function (label) {
    return this.txService.putRelationshipType(label);
}

Transaction.prototype.putAttributeType = function (label, dataType) {
    return this.txService.putAttributeType(label, dataType);
}

Transaction.prototype.putRole = function (label) {
    return this.txService.putRole(label);
}

Transaction.prototype.putRule = function (label, when, then) {
    return this.txService.putRule(label, when, then);
}

Transaction.prototype.close = function () {
    return this.txService.close();
}

module.exports = Transaction;