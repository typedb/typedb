const RequestBuilder = require("./util/RequestBuilder");
const GrpcCommunicator = require("./util/GrpcCommunicator");
const ConceptFactory = require("./concept/ConceptFactory");
const ResponseConverter = require("./util/ResponseConverter");
const GrpcIteratorFactory = require("./util/GrpcIteratorFactory");

/**
 * TransactionService provides implementation of methods that belong to 
 * the transaction rpc method defined in Session.proto
 * 
 * It implements every method using 3 collaborators:
 *  - a gRPC request builder (static)
 *  - a communicator which handles gRPC requests/responses over a duplex Stream
 *  - a converter to convert gRPC responses to valid JS types
 * 
 * @param {Duplex Stream} txStream
 */
function TransactionService(txStream) {
    this.communicator = new GrpcCommunicator(txStream);
    const conceptFactory = new ConceptFactory(this);
    const iteratorFactory = new GrpcIteratorFactory(conceptFactory, this.communicator);
    this.respConverter = new ResponseConverter(conceptFactory, iteratorFactory);
}

// Closes txStream
TransactionService.prototype.close = function () {
    return this.communicator.end();
}

// Concept
TransactionService.prototype.deleteConcept = function (id) {
    const txRequest = RequestBuilder.deleteConcept(id);
    return this.communicator.send(txRequest);
};

// Schema concept
TransactionService.prototype.getLabel = function (id) {
    const txRequest = RequestBuilder.getLabel(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.getLabel(resp));
}
TransactionService.prototype.setLabel = function (id, label) {
    const txRequest = RequestBuilder.setLabel(id, label);
    return this.communicator.send(txRequest);
};
TransactionService.prototype.isImplicit = function (id) {
    const txRequest = RequestBuilder.isImplicit(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.isImplicit(resp));
}
TransactionService.prototype.subs = function (id) {
    const txRequest = RequestBuilder.subs(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.subs(resp));
};
TransactionService.prototype.sups = function (id) {
    const txRequest = RequestBuilder.sups(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.sups(resp));
};
TransactionService.prototype.getSup = function (id) {
    const txRequest = RequestBuilder.getSup(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.getSup(resp));
};
TransactionService.prototype.setSup = function (id, superConcept) {
    const txRequest = RequestBuilder.setSup(id, superConcept);
    return this.communicator.send(txRequest);
};

// Rule 
TransactionService.prototype.getWhen = function (id) {
    const txRequest = RequestBuilder.getWhen(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.getWhen(resp));
};
TransactionService.prototype.getThen = function (id) {
    const txRequest = RequestBuilder.getThen(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.getThen(resp));
};

// Role
TransactionService.prototype.getRelationshipTypesThatRelateRole = function (id) {
    const txRequest = RequestBuilder.getRelationshipTypesThatRelateRole(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.getRelationshipTypesThatRelateRole(resp));
}
TransactionService.prototype.getTypesThatPlayRole = function (id) {
    const txRequest = RequestBuilder.getTypesThatPlayRole(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.getTypesThatPlayRole(resp));
}

// Type
TransactionService.prototype.instances = function (id) {
    const txRequest = RequestBuilder.instances(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.instances(resp));
};
TransactionService.prototype.getAttributeTypes = function (id) {
    const txRequest = RequestBuilder.getAttributeTypes(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.getAttributeTypes(resp));
};
TransactionService.prototype.setAttributeType = function (id, type) {
    const txRequest = RequestBuilder.setAttributeType(id, type);
    return this.communicator.send(txRequest);
};
TransactionService.prototype.unsetAttributeType = function (id, type) {
    const txRequest = RequestBuilder.unsetAttributeType(id, type);
    return this.communicator.send(txRequest);
};
TransactionService.prototype.getKeyTypes = function (id) {
    const txRequest = RequestBuilder.getKeyTypes(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.getKeyTypes(resp));
};
TransactionService.prototype.setKeyType = function (id, keyType) {
    const txRequest = RequestBuilder.setKeyType(id, keyType);
    return this.communicator.send(txRequest);
};
TransactionService.prototype.unsetKeyType = function (id, keyType) {
    const txRequest = RequestBuilder.unsetKeyType(id, keyType);
    return this.communicator.send(txRequest);
};
TransactionService.prototype.isAbstract = function (id) {
    const txRequest = RequestBuilder.isAbstract(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.isAbstract(resp));
};
TransactionService.prototype.setAbstract = function (id, bool) {
    const txRequest = RequestBuilder.setAbstract(id, bool);
    return this.communicator.send(txRequest);
};
TransactionService.prototype.getRolesPlayedByType = function (id) {
    const txRequest = RequestBuilder.getRolesPlayedByType(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.getRolesPlayedByType(resp));
};
TransactionService.prototype.setRolePlayedByType = function (id, role) {
    const txRequest = RequestBuilder.setRolePlayedByType(id, role);
    return this.communicator.send(txRequest);
};
TransactionService.prototype.unsetRolePlayedByType = function (id, role) {
    const txRequest = RequestBuilder.unsetRolePlayedByType(id, role);
    return this.communicator.send(txRequest);
};

// Entity type
TransactionService.prototype.addEntity = function (id) {
    const txRequest = RequestBuilder.addEntity(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.addEntity(resp));
};

// Relationship Type
TransactionService.prototype.addRelationship = function (id) {
    const txRequest = RequestBuilder.addRelationship(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.addRelationship(resp));
};
TransactionService.prototype.getRelatedRoles = function (id) {
    const txRequest = RequestBuilder.getRelatedRoles(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.getRelatedRoles(resp));
};
TransactionService.prototype.setRelatedRole = function (id, role) {
    const txRequest = RequestBuilder.setRelatedRole(id, role);
    return this.communicator.send(txRequest);
};
TransactionService.prototype.unsetRelatedRole = function (id, role) {
    const txRequest = RequestBuilder.unsetRelatedRole(id, role);
    return this.communicator.send(txRequest);
};

// Attribute type
TransactionService.prototype.putAttribute = async function (id, value) {
    const dataTypeTxRequest = RequestBuilder.getDataTypeOfType(id);
    const resp = await this.communicator.send(dataTypeTxRequest);
    const dataType = resp.getConceptmethodRes().getResponse().getAttributetypeDatatypeRes().getDatatype();
    const txRequest = RequestBuilder.putAttribute(id, dataType, value);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.putAttribute(resp));
};
TransactionService.prototype.getAttribute = async function (id, value) {
    const dataTypeTxRequest = RequestBuilder.getDataTypeOfType(id);
    const resp = await this.communicator.send(dataTypeTxRequest);
    const dataType = resp.getConceptmethodRes().getResponse().getAttributetypeDatatypeRes().getDatatype();
    const txRequest = RequestBuilder.getAttribute(id, dataType, value);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.getAttribute(resp));
};
TransactionService.prototype.getDataTypeOfType = function (id) {
    const txRequest = RequestBuilder.getDataTypeOfType(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.getDataTypeOfType(resp));
};
TransactionService.prototype.getRegex = function (id) {
    const txRequest = RequestBuilder.getRegex(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.getRegex(resp));
};
TransactionService.prototype.setRegex = function (id, regex) {
    const txRequest = RequestBuilder.setRegex(id, regex);
    return this.communicator.send(txRequest);
};

//Thing
TransactionService.prototype.isInferred = function (id) {
    const txRequest = RequestBuilder.isInferred(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.isInferred(resp));
};
TransactionService.prototype.getDirectType = function (id) {
    const txRequest = RequestBuilder.getDirectType(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.getDirectType(response));
};
TransactionService.prototype.getRelationshipsByRoles = function (id, roles) {
    const txRequest = RequestBuilder.getRelationshipsByRoles(id, roles);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.getRelationshipsByRoles(response));
};
TransactionService.prototype.getRolesPlayedByThing = function (id) {
    const txRequest = RequestBuilder.getRolesPlayedByThing(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.getRolesPlayedByThing(response));
};
TransactionService.prototype.getAttributesByTypes = function (id, types) {
    const txRequest = RequestBuilder.getAttributesByTypes(id, types);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.getAttributesByTypes(response));
};
TransactionService.prototype.getKeysByTypes = function (id, types) {
    const txRequest = RequestBuilder.getKeysByTypes(id, types);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.getKeysByTypes(response));
};
TransactionService.prototype.setAttribute = function (id, attribute) {
    const txRequest = RequestBuilder.setAttribute(id, attribute);
    return this.communicator.send(txRequest);
};
TransactionService.prototype.unsetAttribute = function (id, attribute) {
    const txRequest = RequestBuilder.unsetAttribute(id, attribute);
    return this.communicator.send(txRequest);
};

// Relationship
TransactionService.prototype.rolePlayersMap = function (id) {
    const txRequest = RequestBuilder.rolePlayersMap(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.rolePlayersMap(response));
};
TransactionService.prototype.rolePlayers = function (id, roles) {
    const txRequest = RequestBuilder.rolePlayers(id, roles);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.rolePlayers(response));
};
TransactionService.prototype.setRolePlayer = function (id, role, thing) {
    const txRequest = RequestBuilder.setRolePlayer(id, role, thing);
    return this.communicator.send(txRequest);
};
TransactionService.prototype.unsetRolePlayer = function (id, role, thing) {
    const txRequest = RequestBuilder.unsetRolePlayer(id, role, thing);
    return this.communicator.send(txRequest);
};
// Attribute
TransactionService.prototype.getValue = function (id) {
    const txRequest = RequestBuilder.getValue(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.getValue(response));
};
TransactionService.prototype.getOwners = function (id) {
    const txRequest = RequestBuilder.getOwners(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.getOwners(response));
};

// ======================= Grakn transaction methods ========================= //

TransactionService.prototype.getConcept = function (conceptId) {
    const txRequest = RequestBuilder.getConcept(conceptId);
    return this.communicator.send(txRequest)
        .then((response) => this.respConverter.getConcept(response));
}

TransactionService.prototype.getSchemaConcept = function (label) {
    const txRequest = RequestBuilder.getSchemaConcept(label);
    return this.communicator.send(txRequest)
        .then((response) => this.respConverter.getSchemaConcept(response));
}

TransactionService.prototype.putEntityType = function (label) {
    const txRequest = RequestBuilder.putEntityType(label);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.putEntityType(response));
}

TransactionService.prototype.putRelationshipType = function (label) {
    const txRequest = RequestBuilder.putRelationshipType(label);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.putRelationshipType(response));
}

TransactionService.prototype.putRole = function (label) {
    const txRequest = RequestBuilder.putRole(label);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.putRole(response));
}

TransactionService.prototype.putRule = function (label, when, then) {
    const txRequest = RequestBuilder.putRule(label, when, then);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.putRule(response));
}

TransactionService.prototype.putAttributeType = function (label, dataType) {
    const txRequest = RequestBuilder.putAttributeType(label, dataType);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.putAttributeType(response));
}

TransactionService.prototype.getAttributesByValue = function (value, dataType) {
    const txRequest = RequestBuilder.getAttributesByValue(value, dataType);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.getAttributesByValue(response));
}

TransactionService.prototype.openTx = function (keyspace, txType, credentials) {
    const txRequest = RequestBuilder.openTx(keyspace, txType, credentials);
    return this.communicator.send(txRequest);
};

TransactionService.prototype.commit = function () {
    const txRequest = RequestBuilder.commit();
    return this.communicator.send(txRequest);
}

TransactionService.prototype.query = function executeQuery(query, options) {
    const txRequest = RequestBuilder.executeQuery(query, options);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.executeQuery(resp));
};

module.exports = TransactionService;