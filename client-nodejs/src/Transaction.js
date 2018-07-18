

/**
 * This is produced by Grakn and allows the user to construct and perform
 * basic look ups to the knowledge base. This also allows the execution of Graql queries.
 * 
 * @param {Object} txService Object implementing all the functionalities of gRPC Transaction service as defined in grakn.proto
 */
function Transaction(txService) {
    this.txService = txService;
}

/**
 * Executes a given Graql query on the current keyspace
 * @param {String} query String representing a Graql query 
 */
Transaction.prototype.query = function executeQuery(query, options) {
    return this.txService.query(query, options);
};

/**
 * Commits any changes to the graph and closes the transaction. The user must use the GraknSession object to
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