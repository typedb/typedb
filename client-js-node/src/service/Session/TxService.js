const TxRequestBuilder = require("./util/TxRequestBuilder");
const GrpcCommunicator = require("./util/GrpcCommunicator");
const ConceptFactory = require("../../concept/ConceptFactory");
const ResponseConverter = require("./util/ResponseConverter");

/**
 * TxServiceexecutes the methods provided by the gRPC that are defined
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
    const txRequest = TxRequestBuilder.deleteConcept(id);
    return this.communicator.send(txRequest);
};

// Schema concept
TxService.prototype.getLabel = function (id) {
    const txRequest = TxRequestBuilder.getLabel(id);
    return this.communicator.send(txRequest)
        .then(resp => resp.getConceptresponse().getLabel().getValue());
}
TxService.prototype.setLabel = function (id, label) {
    const txRequest = TxRequestBuilder.setLabel(id, label);
    return this.communicator.send(txRequest);
};
TxService.prototype.isImplicit = function (id) {
    const txRequest = TxRequestBuilder.isImplicit(id);
    return this.communicator.send(txRequest)
        .then(resp => resp.getConceptresponse().getBool());
}
TxService.prototype.getSubConcepts = function (id) {
    const txRequest = TxRequestBuilder.getSubConcepts(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
};
TxService.prototype.getSuperConcepts = function (id) {
    const txRequest = TxRequestBuilder.getSuperConcepts(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
};
TxService.prototype.getDirectSuperConcept = function (id) {
    const txRequest = TxRequestBuilder.getDirectSuperConcept(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptFromOptional(response));
};
TxService.prototype.setDirectSuperConcept = function (id, superConcept) {
    const txRequest = TxRequestBuilder.setDirectSuperConcept(id, superConcept);
    return this.communicator.send(txRequest);
};

// Rule 
TxService.prototype.getWhen = function (id) {
    const txRequest = TxRequestBuilder.getWhen(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.getOptionalPattern(response));
};
TxService.prototype.getThen = function (id) {
    const txRequest = TxRequestBuilder.getThen(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.getOptionalPattern(response));
};

// Role
TxService.prototype.getRelationshipTypesThatRelateRole = function (id) {
    const txRequest = TxRequestBuilder.getRelationshipTypesThatRelateRole(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
}
TxService.prototype.getTypesThatPlayRole = function (id) {
    const txRequest = TxRequestBuilder.getTypesThatPlayRole(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
}

// Type
TxService.prototype.getInstances = function (id) {
    const txRequest = TxRequestBuilder.getInstances(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
};
TxService.prototype.getAttributeTypes = function (id) {
    const txRequest = TxRequestBuilder.getAttributeTypes(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
};
TxService.prototype.setAttributeType = function (id, type) {
    const txRequest = TxRequestBuilder.setAttributeType(id, type);
    return this.communicator.send(txRequest);
};
TxService.prototype.unsetAttributeType = function (id, type) {
    const txRequest = TxRequestBuilder.unsetAttributeType(id, type);
    return this.communicator.send(txRequest);
};
TxService.prototype.getKeyTypes = function (id) {
    const txRequest = TxRequestBuilder.getKeyTypes(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
};
TxService.prototype.setKeyType = function (id, keyType) {
    const txRequest = TxRequestBuilder.setKeyType(id, keyType);
    return this.communicator.send(txRequest);
};
TxService.prototype.unsetKeyType = function (id, keyType) {
    const txRequest = TxRequestBuilder.unsetKeyType(id, keyType);
    return this.communicator.send(txRequest);
};
TxService.prototype.isAbstract = function (id) {
    const txRequest = TxRequestBuilder.isAbstract(id);
    return this.communicator.send(txRequest)
        .then(resp => resp.getConceptresponse().getBool());
};
TxService.prototype.setAbstract = function (id, bool) {
    const txRequest = TxRequestBuilder.setAbstract(id, bool);
    return this.communicator.send(txRequest);
};
TxService.prototype.getRolesPlayedByType = function (id) {
    const txRequest = TxRequestBuilder.getRolesPlayedByType(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
};
TxService.prototype.setRolePlayedByType = function (id, role) {
    const txRequest = TxRequestBuilder.setRolePlayedByType(id, role);
    return this.communicator.send(txRequest);
};
TxService.prototype.unsetRolePlayedByType = function (id, role) {
    const txRequest = TxRequestBuilder.unsetRolePlayedByType(id, role);
    return this.communicator.send(txRequest);
};

// Entity type
TxService.prototype.addEntity = function (id) {
    const txRequest = TxRequestBuilder.addEntity(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptFromResponse(response));
};

// Relationship Type
TxService.prototype.addRelationship = function (id) {
    const txRequest = TxRequestBuilder.addRelationship(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptFromResponse(response));
};
TxService.prototype.getRelatedRoles = function (id) {
    const txRequest = TxRequestBuilder.getRelatedRoles(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
};
TxService.prototype.setRelatedRole = function (id, role) {
    const txRequest = TxRequestBuilder.setRelatedRole(id, role);
    return this.communicator.send(txRequest);
};
TxService.prototype.unsetRelatedRole = function (id, role) {
    const txRequest = TxRequestBuilder.unsetRelatedRole(id, role);
    return this.communicator.send(txRequest);
};

// Attribute type
TxService.prototype.putAttribute = async function (id, value) {
    const dataTypeTxRequest = TxRequestBuilder.getDataTypeOfType(id);
    const resp = await this.communicator.send(dataTypeTxRequest);
    const dataType = resp.getConceptmethodRes().getResponse().getAttributetypeDatatypeRes().getDatatype();
    const txRequest = TxRequestBuilder.putAttribute(id, dataType, value);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.putAttribute(resp));
};
TxService.prototype.getAttribute = async function (id, value) {
    const dataTypeTxRequest = TxRequestBuilder.getDataTypeOfType(id);
    const resp = await this.communicator.send(dataTypeTxRequest);
    const dataType = resp.getConceptresponse().getOptionaldatatype().getPresent();
    const txRequest = TxRequestBuilder.getAttribute(id, dataType, value);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptFromOptional(response));
};
TxService.prototype.getDataTypeOfType = function (id) {
    const txRequest = TxRequestBuilder.getDataTypeOfType(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.getOptionalDataType(response));
};
TxService.prototype.getRegex = function (id) {
    const txRequest = TxRequestBuilder.getRegex(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.getOptionalRegex(response));
};
TxService.prototype.setRegex = function (id, regex) {
    const txRequest = TxRequestBuilder.setRegex(id, regex);
    return this.communicator.send(txRequest);
};

//Thing
TxService.prototype.isInferred = function (id) {
    const txRequest = TxRequestBuilder.isInferred(id);
    return this.communicator.send(txRequest)
        .then(resp => resp.getConceptresponse().getBool());
};
TxService.prototype.getDirectType = function (id) {
    const txRequest = TxRequestBuilder.getDirectType(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptFromResponse(response));
};
TxService.prototype.getRelationships = function (id) {
    const txRequest = TxRequestBuilder.getRelationships(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
}
TxService.prototype.getRelationshipsByRoles = function (id, roles) {
    const txRequest = TxRequestBuilder.getRelationshipsByRoles(id, roles);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
};
TxService.prototype.getRolesPlayedByThing = function (id) {
    const txRequest = TxRequestBuilder.getRolesPlayedByThing(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
};
TxService.prototype.getAttributes = function (id) {
    const txRequest = TxRequestBuilder.getAttributes(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
};
TxService.prototype.getAttributesByTypes = function (id, types) {
    const txRequest = TxRequestBuilder.getAttributesByTypes(id, types);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
};
TxService.prototype.getKeys = function (id) {
    const txRequest = TxRequestBuilder.getKeys(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
};
TxService.prototype.getKeysByTypes = function (id, types) {
    const txRequest = TxRequestBuilder.getKeysByTypes(id, types);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
};
TxService.prototype.setAttribute = function (id, attribute) {
    const txRequest = TxRequestBuilder.setAttribute(id, attribute);
    return this.communicator.send(txRequest);
};
TxService.prototype.unsetAttribute = function (id, attribute) {
    const txRequest = TxRequestBuilder.unsetAttribute(id, attribute);
    return this.communicator.send(txRequest);
};

// Relationship
TxService.prototype.getRolePlayers = function (id) {
    const txRequest = TxRequestBuilder.getRolePlayers(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.consumeRolePlayerIterator(response));
};
TxService.prototype.getRolePlayersByRoles = function (id, roles) {
    const txRequest = TxRequestBuilder.getRolePlayersByRoles(id, roles);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
};
TxService.prototype.setRolePlayer = function (id, role, thing) {
    const txRequest = TxRequestBuilder.setRolePlayer(id, role, thing);
    return this.communicator.send(txRequest);
};
TxService.prototype.unsetRolePlayer = function (id, role, thing) {
    const txRequest = TxRequestBuilder.unsetRolePlayer(id, role, thing);
    return this.communicator.send(txRequest);
};
// Attribute
TxService.prototype.getValue = function (id) {
    const txRequest = TxRequestBuilder.getValue(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.getValue(response));
};
TxService.prototype.getOwners = function (id) {
    const txRequest = TxRequestBuilder.getOwners(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
};

TxService.prototype.getDataTypeOfAttribute = function (id) {
    const txRequest = TxRequestBuilder.getDataTypeOfAttribute(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.dataTypeToString(response.getConceptresponse().getDatatype()));
};

// ======================= Grakn transaction methods ========================= //

TxService.prototype.getConcept = function (conceptId) {
    const txRequest = TxRequestBuilder.getConcept(conceptId);
    return this.communicator.send(txRequest)
        .then((response) => this.respConverter.conceptFromOptional(response));
}

TxService.prototype.getSchemaConcept = function (label) {
    const txRequest = TxRequestBuilder.getSchemaConcept(label);
    return this.communicator.send(txRequest)
        .then((response) => this.respConverter.conceptFromOptional(response));
}

TxService.prototype.putEntityType = function (label) {
    const txRequest = TxRequestBuilder.putEntityType(label);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptFromResponse(response));
}

TxService.prototype.putRelationshipType = function (label) {
    const txRequest = TxRequestBuilder.putRelationshipType(label);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptFromResponse(response));
}

TxService.prototype.putRole = function (label) {
    const txRequest = TxRequestBuilder.putRole(label);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptFromResponse(response));
}

TxService.prototype.putRule = function (label, when, then) {
    const txRequest = TxRequestBuilder.putRule(label, when, then);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptFromResponse(response));
}

TxService.prototype.putAttributeType = function (label, dataType) {
    const txRequest = TxRequestBuilder.putAttributeType(label, dataType);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.putAttributeType(response));
}

TxService.prototype.getAttributesByValue = function (value, dataType) {
    const txRequest = TxRequestBuilder.getAttributesByValue(value, dataType);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
}

TxService.prototype.openTx = function (keyspace, txType, credentials) {
    const txRequest = TxRequestBuilder.openTx(keyspace, txType, credentials);
    return this.communicator.send(txRequest);
};

TxService.prototype.commit = function () {
    const txRequest = TxRequestBuilder.commit();
    return this.communicator.send(txRequest);
}

TxService.prototype.execute = function executeQuery(query) {
    const txRequest = TxRequestBuilder.executeQuery(query);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.executeResponse(resp));
};

module.exports = TxService;