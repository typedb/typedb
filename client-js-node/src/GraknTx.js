

/**
 * This is produced by Grakn and allows the user to construct and perform
 * basic look ups to the knowledge base. This also allows the execution of Graql queries.
 * 
 * @param {Object} txService Object implementing all the functionalities of gRPC Tx service as defined in grakn.proto
 */
function GraknTx(txService) {
    this.txService = txService;
}

/**
 * Executes a given Graql query on the current keyspace
 * @param {String} query String representing a Graql query 
 */
GraknTx.prototype.execute = function executeQuery(query) {
    return this.txService.execute(query);
};

/**
 * Commits any changes to the graph and closes the transaction. The user must use the GraknSession object to
 * get a new open transaction.
 */
GraknTx.prototype.commit = async function () {
    await this.txService.commit();
    return this.close();
}

/**
 * Get the Concept with identifier provided, if it exists.
 * 
 * @param {String} conceptId A unique identifier for the Concept in the graph.
 */
GraknTx.prototype.getConcept = function (conceptId) {
    return this.txService.getConcept(conceptId);
}

GraknTx.prototype.getSchemaConcept = function (label) {
    return this.txService.getSchemaConcept(label);
}

GraknTx.prototype.getAttributesByValue = function (attributeValue, dataType) {
    return this.txService.getAttributesByValue(attributeValue, dataType);
}

GraknTx.prototype.putEntityType = function (label) {
    return this.txService.putEntityType(label);
}

GraknTx.prototype.putRelationshipType = function (label) {
    return this.txService.putRelationshipType(label);
}

GraknTx.prototype.putAttributeType = function (value, dataType) {
    return this.txService.putAttributeType(value, dataType);
}

GraknTx.prototype.putRole = function (label) {
    return this.txService.putRole(label);
}

GraknTx.prototype.putRule = function (label, when, then) {
    return this.txService.putRule(label, when, then);
}

GraknTx.prototype.close = function () {
    return this.txService.close();
}

module.exports = GraknTx;