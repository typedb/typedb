const RequestBuilder = require("./util/RequestBuilder");
const GrpcCommunicator = require("./util/GrpcCommunicator");
const ConceptFactory = require("./concept/ConceptFactory");
const ResponseConverter = require("./util/ResponseConverter");

/**
 * TxService executes the methods provided by the gRPC that are defined
 * inside concept.proto and returns responses in JS data types.
 * It has 2 collaborators:
 *  - a communicator which handles gRPC requests/responses over a duplex Stream
 *  - a converter to convert gRPC responses to valid JS types
 */
function TxService(txStream) {
    this.communicator = new GrpcCommunicator(txStream);
    this.respConverter = new ResponseConverter(new ConceptFactory(this), this.communicator);
}

// Closes txStream
TxService.prototype.close = function () {
    return this.communicator.end();
}

// Concept
TxService.prototype.deleteConcept = function (id) {
    const txRequest = RequestBuilder.deleteConcept(id);
    return this.communicator.send(txRequest);
};

// Schema concept
TxService.prototype.getLabel = function (id) {
    const txRequest = RequestBuilder.getLabel(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.getLabel(resp));
}
TxService.prototype.setLabel = function (id, label) {
    const txRequest = RequestBuilder.setLabel(id, label);
    return this.communicator.send(txRequest);
};
TxService.prototype.isImplicit = function (id) {
    const txRequest = RequestBuilder.isImplicit(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.isImplicit(resp));
}
TxService.prototype.subs = function (id) {
    const txRequest = RequestBuilder.subs(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.subs(resp));
};
TxService.prototype.sups = function (id) {
    const txRequest = RequestBuilder.sups(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.sups(resp));
};
TxService.prototype.getSup = function (id) {
    const txRequest = RequestBuilder.getSup(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.getSup(resp));
};
TxService.prototype.setSup = function (id, superConcept) {
    const txRequest = RequestBuilder.setSup(id, superConcept);
    return this.communicator.send(txRequest);
};

// Rule 
TxService.prototype.getWhen = function (id) {
    const txRequest = RequestBuilder.getWhen(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.getWhen(resp));
};
TxService.prototype.getThen = function (id) {
    const txRequest = RequestBuilder.getThen(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.getThen(resp));
};

// Role
TxService.prototype.getRelationshipTypesThatRelateRole = function (id) {
    const txRequest = RequestBuilder.getRelationshipTypesThatRelateRole(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.getRelationshipTypesThatRelateRole(resp));
}
TxService.prototype.getTypesThatPlayRole = function (id) {
    const txRequest = RequestBuilder.getTypesThatPlayRole(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.getTypesThatPlayRole(resp));
}

// Type
TxService.prototype.instances = function (id) {
    const txRequest = RequestBuilder.instances(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.instances(resp));
};
TxService.prototype.getAttributeTypes = function (id) {
    const txRequest = RequestBuilder.getAttributeTypes(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.getAttributeTypes(resp));
};
TxService.prototype.setAttributeType = function (id, type) {
    const txRequest = RequestBuilder.setAttributeType(id, type);
    return this.communicator.send(txRequest);
};
TxService.prototype.unsetAttributeType = function (id, type) {
    const txRequest = RequestBuilder.unsetAttributeType(id, type);
    return this.communicator.send(txRequest);
};
TxService.prototype.getKeyTypes = function (id) {
    const txRequest = RequestBuilder.getKeyTypes(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.getKeyTypes(resp));
};
TxService.prototype.setKeyType = function (id, keyType) {
    const txRequest = RequestBuilder.setKeyType(id, keyType);
    return this.communicator.send(txRequest);
};
TxService.prototype.unsetKeyType = function (id, keyType) {
    const txRequest = RequestBuilder.unsetKeyType(id, keyType);
    return this.communicator.send(txRequest);
};
TxService.prototype.isAbstract = function (id) {
    const txRequest = RequestBuilder.isAbstract(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.isAbstract(resp));
};
TxService.prototype.setAbstract = function (id, bool) {
    const txRequest = RequestBuilder.setAbstract(id, bool);
    return this.communicator.send(txRequest);
};
TxService.prototype.getRolesPlayedByType = function (id) {
    const txRequest = RequestBuilder.getRolesPlayedByType(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.getRolesPlayedByType(resp));
};
TxService.prototype.setRolePlayedByType = function (id, role) {
    const txRequest = RequestBuilder.setRolePlayedByType(id, role);
    return this.communicator.send(txRequest);
};
TxService.prototype.unsetRolePlayedByType = function (id, role) {
    const txRequest = RequestBuilder.unsetRolePlayedByType(id, role);
    return this.communicator.send(txRequest);
};

// Entity type
TxService.prototype.addEntity = function (id) {
    const txRequest = RequestBuilder.addEntity(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.addEntity(resp));
};

// Relationship Type
TxService.prototype.addRelationship = function (id) {
    const txRequest = RequestBuilder.addRelationship(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.addRelationship(resp));
};
TxService.prototype.getRelatedRoles = function (id) {
    const txRequest = RequestBuilder.getRelatedRoles(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.getRelatedRoles(resp));
};
TxService.prototype.setRelatedRole = function (id, role) {
    const txRequest = RequestBuilder.setRelatedRole(id, role);
    return this.communicator.send(txRequest);
};
TxService.prototype.unsetRelatedRole = function (id, role) {
    const txRequest = RequestBuilder.unsetRelatedRole(id, role);
    return this.communicator.send(txRequest);
};

// Attribute type
TxService.prototype.putAttribute = async function (id, value) {
    const dataTypeTxRequest = RequestBuilder.getDataTypeOfType(id);
    const resp = await this.communicator.send(dataTypeTxRequest);
    const dataType = resp.getConceptmethodRes().getResponse().getAttributetypeDatatypeRes().getDatatype();
    const txRequest = RequestBuilder.putAttribute(id, dataType, value);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.putAttribute(resp));
};
TxService.prototype.getAttribute = async function (id, value) {
    const dataTypeTxRequest = RequestBuilder.getDataTypeOfType(id);
    const resp = await this.communicator.send(dataTypeTxRequest);
    const dataType = resp.getConceptmethodRes().getResponse().getAttributetypeDatatypeRes().getDatatype();
    const txRequest = RequestBuilder.getAttribute(id, dataType, value);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.getAttribute(resp));
};
TxService.prototype.getDataTypeOfType = function (id) {
    const txRequest = RequestBuilder.getDataTypeOfType(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.getDataTypeOfType(resp));
};
TxService.prototype.getRegex = function (id) {
    const txRequest = RequestBuilder.getRegex(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.getRegex(resp));
};
TxService.prototype.setRegex = function (id, regex) {
    const txRequest = RequestBuilder.setRegex(id, regex);
    return this.communicator.send(txRequest);
};

//Thing
TxService.prototype.isInferred = function (id) {
    const txRequest = RequestBuilder.isInferred(id);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.isInferred(resp));
};
TxService.prototype.getDirectType = function (id) {
    const txRequest = RequestBuilder.getDirectType(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.getDirectType(response));
};
TxService.prototype.getRelationshipsByRoles = function (id, roles) {
    const txRequest = RequestBuilder.getRelationshipsByRoles(id, roles);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.getRelationshipsByRoles(response));
};
TxService.prototype.getRolesPlayedByThing = function (id) {
    const txRequest = RequestBuilder.getRolesPlayedByThing(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.getRolesPlayedByThing(response));
};
TxService.prototype.getAttributesByTypes = function (id, types) {
    const txRequest = RequestBuilder.getAttributesByTypes(id, types);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.getAttributesByTypes(response));
};
TxService.prototype.getKeysByTypes = function (id, types) {
    const txRequest = RequestBuilder.getKeysByTypes(id, types);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.getKeysByTypes(response));
};
TxService.prototype.setAttribute = function (id, attribute) {
    const txRequest = RequestBuilder.setAttribute(id, attribute);
    return this.communicator.send(txRequest);
};
TxService.prototype.unsetAttribute = function (id, attribute) {
    const txRequest = RequestBuilder.unsetAttribute(id, attribute);
    return this.communicator.send(txRequest);
};

// Relationship
TxService.prototype.rolePlayersMap = function (id) {
    const txRequest = RequestBuilder.rolePlayersMap(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.rolePlayersMap(response));
};
TxService.prototype.rolePlayers = function (id, roles) {
    const txRequest = RequestBuilder.rolePlayers(id, roles);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.rolePlayers(response));
};
TxService.prototype.setRolePlayer = function (id, role, thing) {
    const txRequest = RequestBuilder.setRolePlayer(id, role, thing);
    return this.communicator.send(txRequest);
};
TxService.prototype.unsetRolePlayer = function (id, role, thing) {
    const txRequest = RequestBuilder.unsetRolePlayer(id, role, thing);
    return this.communicator.send(txRequest);
};
// Attribute
TxService.prototype.getValue = function (id) {
    const txRequest = RequestBuilder.getValue(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.getValue(response));
};
TxService.prototype.getOwners = function (id) {
    const txRequest = RequestBuilder.getOwners(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.getOwners(response));
};

// ======================= Grakn transaction methods ========================= //

TxService.prototype.getConcept = function (conceptId) {
    const txRequest = RequestBuilder.getConcept(conceptId);
    return this.communicator.send(txRequest)
        .then((response) => this.respConverter.getConcept(response));
}

TxService.prototype.getSchemaConcept = function (label) {
    const txRequest = RequestBuilder.getSchemaConcept(label);
    return this.communicator.send(txRequest)
        .then((response) => this.respConverter.getSchemaConcept(response));
}

TxService.prototype.putEntityType = function (label) {
    const txRequest = RequestBuilder.putEntityType(label);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.putEntityType(response));
}

TxService.prototype.putRelationshipType = function (label) {
    const txRequest = RequestBuilder.putRelationshipType(label);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.putRelationshipType(response));
}

TxService.prototype.putRole = function (label) {
    const txRequest = RequestBuilder.putRole(label);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.putRole(response));
}

TxService.prototype.putRule = function (label, when, then) {
    const txRequest = RequestBuilder.putRule(label, when, then);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.putRule(response));
}

TxService.prototype.putAttributeType = function (label, dataType) {
    const txRequest = RequestBuilder.putAttributeType(label, dataType);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.putAttributeType(response));
}

TxService.prototype.getAttributesByValue = function (value, dataType) {
    const txRequest = RequestBuilder.getAttributesByValue(value, dataType);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.getAttributesByValue(response));
}

TxService.prototype.openTx = function (keyspace, txType, credentials) {
    const txRequest = RequestBuilder.openTx(keyspace, txType, credentials);
    return this.communicator.send(txRequest);
};

TxService.prototype.commit = function () {
    const txRequest = RequestBuilder.commit();
    return this.communicator.send(txRequest);
}

TxService.prototype.query = function executeQuery(query) {
    const txRequest = RequestBuilder.executeQuery(query);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.executeQuery(resp));
};

module.exports = TxService;