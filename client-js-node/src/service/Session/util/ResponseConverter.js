const GrpcIteratorFactory = require("./GrpcIteratorFactory");

function ResponseConverter(conceptFactory, communicator) {
    this.iteratorFactory = new GrpcIteratorFactory(communicator);
    this.conceptFactory = conceptFactory;
}

/**
 * This method creates and consumes an iterator (until server returns Done) and build Concept object from
 * every response.
 * 
 * Used both with ConceptResponse and TxResponse, they both carry IteratorId, but nested differently.
 * 
 * @param {*} grpcResponse gRPC response that will contain iteratorId
 * @param {*} txService txService implementation needed to be injected to new concepts that will be built
 */
ResponseConverter.prototype.conceptsFromIterator = async function (grpcResponse) {
    const iteratorId = (grpcResponse.hasConceptresponse()) ?
        grpcResponse.getConceptresponse().getIteratorid() :
        grpcResponse.getIteratorid();
    const iterator = this.iteratorFactory.createConceptIterator(iteratorId);
    const concepts = [];
    let concept = await iterator.nextResult();
    while (concept) {
        concepts.push(this.conceptFactory.createConcept(concept));
        concept = await iterator.nextResult();
    }
    return concepts;
}

ResponseConverter.prototype.conceptFromResponse = function (response) {
    const concept = (response.hasConceptresponse()) ?
        response.getConceptresponse().getConcept() :
        response.getConcept();
    return this.conceptFactory.createConcept(concept)
}

ResponseConverter.prototype.conceptFromOptional = function (response) {
    const optionalConcept = (response.hasConceptresponse()) ?
        response.getConceptresponse().getOptionalconcept() :
        response.getOptionalconcept();
    return (optionalConcept.hasPresent()) ?
        this.conceptFactory.createConcept(optionalConcept.getPresent()) :
        null;
}

ResponseConverter.prototype.consumeRolePlayerIterator = async function (grpcConceptResponse) {
    const iteratorId = grpcConceptResponse.getConceptresponse().getIteratorid();
    const iterator = this.iteratorFactory.createRolePlayerIterator(iteratorId);
    const rolePlayers = [];
    let grpcRolePlayer = await iterator.nextResult();
    while (grpcRolePlayer) {
        rolePlayers.push({
            role: this.conceptFactory.createConcept(grpcRolePlayer.getRole()),
            player: this.conceptFactory.createConcept(grpcRolePlayer.getPlayer())
        });
        grpcRolePlayer = await iterator.nextResult();
    }
    return rolePlayers;
}

ResponseConverter.prototype.dataTypeToString = function (dataType) {
    switch (dataType) {
        case 0: return "String";
        case 1: return "Boolean";
        case 2: return "Integer";
        case 3: return "Long";
        case 4: return "Float";
        case 5: return "Double";
        case 6: return "Date";
    }
}


ResponseConverter.prototype.getOptionalRegex = function (response) {
    const optionalRegex = response.getConceptresponse().getOptionalregex();
    return (optionalRegex.hasPresent()) ?
        optionalRegex.getPresent() :
        null;
}

ResponseConverter.prototype.getOptionalPattern = function (response) {
    const optionalPattern = response.getConceptresponse().getOptionalpattern();
    return (optionalPattern.hasPresent()) ?
        optionalPattern.getPresent().getValue() :
        null;
}

ResponseConverter.prototype.getOptionalDataType = function (response) {
    const optionalDatatype = response.getConceptresponse().getOptionaldatatype();
    return (optionalDatatype.hasPresent()) ?
        this.dataTypeToString(optionalDatatype.getPresent()) :
        null;
}


function parseQueryResult(queryResult, factory) {
    if (queryResult.hasOtherresult()) {
        // compute or aggregate query
        return JSON.parse(queryResult.getOtherresult());
    } else {
        const answerMap = new Map();
        queryResult
            .getAnswer()
            .getAnswerMap()
            .forEach((grpcConcept, key) => {
                answerMap.set(key, factory.createConcept(grpcConcept));
            });
        return answerMap;
    }
};

ResponseConverter.prototype.executeResponse = async function (resp) {
    const resultArray = [];
    if (resp.hasIteratorid()) {
        const iterator = this.iteratorFactory.createQueryIterator(resp.getIteratorid());
        let nextResult = await iterator.nextResult();
        while (nextResult) {
            const parsedResult = parseQueryResult(nextResult, this.conceptFactory);
            resultArray.push(parsedResult);
            nextResult = await iterator.nextResult();
        }
    }
    if (resp.hasQueryresult()) {
        const queryResult = resp.getQueryresult();
        const parsedResult = parseQueryResult(queryResult, this.conceptFactory);
        resultArray.push(parsedResult);
    }
    return resultArray;
};

ResponseConverter.prototype.putAttributeType = function (resp) {
    const concept = resp.getPutattributetypeRes().getConcept();
    return this.conceptFactory.createConcept(concept);
}

// Concept
ResponseConverter.prototype.deleteConcept = function (id) {
    const txRequest = TxRequestBuilder.deleteConcept(id);
    return this.communicator.send(txRequest);
};

// Schema concept
ResponseConverter.prototype.getLabel = function (id) {
    const txRequest = TxRequestBuilder.getLabel(id);
    return this.communicator.send(txRequest)
        .then(resp => resp.getConceptresponse().getLabel().getValue());
}
ResponseConverter.prototype.setLabel = function (id, label) {
    const txRequest = TxRequestBuilder.setLabel(id, label);
    return this.communicator.send(txRequest);
};
ResponseConverter.prototype.isImplicit = function (id) {
    const txRequest = TxRequestBuilder.isImplicit(id);
    return this.communicator.send(txRequest)
        .then(resp => resp.getConceptresponse().getBool());
}
ResponseConverter.prototype.getSubConcepts = function (id) {
    const txRequest = TxRequestBuilder.getSubConcepts(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
};
ResponseConverter.prototype.getSuperConcepts = function (id) {
    const txRequest = TxRequestBuilder.getSuperConcepts(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
};
ResponseConverter.prototype.getDirectSuperConcept = function (id) {
    const txRequest = TxRequestBuilder.getDirectSuperConcept(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptFromOptional(response));
};
ResponseConverter.prototype.setDirectSuperConcept = function (id, superConcept) {
    const txRequest = TxRequestBuilder.setDirectSuperConcept(id, superConcept);
    return this.communicator.send(txRequest);
};

// Rule 
ResponseConverter.prototype.getWhen = function (id) {
    const txRequest = TxRequestBuilder.getWhen(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.getOptionalPattern(response));
};
ResponseConverter.prototype.getThen = function (id) {
    const txRequest = TxRequestBuilder.getThen(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.getOptionalPattern(response));
};

// Role
ResponseConverter.prototype.getRelationshipTypesThatRelateRole = function (id) {
    const txRequest = TxRequestBuilder.getRelationshipTypesThatRelateRole(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
}
ResponseConverter.prototype.getTypesThatPlayRole = function (id) {
    const txRequest = TxRequestBuilder.getTypesThatPlayRole(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
}

// Type
ResponseConverter.prototype.getInstances = function (id) {
    const txRequest = TxRequestBuilder.getInstances(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
};
ResponseConverter.prototype.getAttributeTypes = function (id) {
    const txRequest = TxRequestBuilder.getAttributeTypes(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
};
ResponseConverter.prototype.setAttributeType = function (id, type) {
    const txRequest = TxRequestBuilder.setAttributeType(id, type);
    return this.communicator.send(txRequest);
};
ResponseConverter.prototype.unsetAttributeType = function (id, type) {
    const txRequest = TxRequestBuilder.unsetAttributeType(id, type);
    return this.communicator.send(txRequest);
};
ResponseConverter.prototype.getKeyTypes = function (id) {
    const txRequest = TxRequestBuilder.getKeyTypes(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
};
ResponseConverter.prototype.setKeyType = function (id, keyType) {
    const txRequest = TxRequestBuilder.setKeyType(id, keyType);
    return this.communicator.send(txRequest);
};
ResponseConverter.prototype.unsetKeyType = function (id, keyType) {
    const txRequest = TxRequestBuilder.unsetKeyType(id, keyType);
    return this.communicator.send(txRequest);
};
ResponseConverter.prototype.isAbstract = function (id) {
    const txRequest = TxRequestBuilder.isAbstract(id);
    return this.communicator.send(txRequest)
        .then(resp => resp.getConceptresponse().getBool());
};
ResponseConverter.prototype.setAbstract = function (id, bool) {
    const txRequest = TxRequestBuilder.setAbstract(id, bool);
    return this.communicator.send(txRequest);
};
ResponseConverter.prototype.getRolesPlayedByType = function (id) {
    const txRequest = TxRequestBuilder.getRolesPlayedByType(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
};
ResponseConverter.prototype.setRolePlayedByType = function (id, role) {
    const txRequest = TxRequestBuilder.setRolePlayedByType(id, role);
    return this.communicator.send(txRequest);
};
ResponseConverter.prototype.unsetRolePlayedByType = function (id, role) {
    const txRequest = TxRequestBuilder.unsetRolePlayedByType(id, role);
    return this.communicator.send(txRequest);
};

// Entity type
ResponseConverter.prototype.addEntity = function (id) {
    const txRequest = TxRequestBuilder.addEntity(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptFromResponse(response));
};

// Relationship Type
ResponseConverter.prototype.addRelationship = function (id) {
    const txRequest = TxRequestBuilder.addRelationship(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptFromResponse(response));
};
ResponseConverter.prototype.getRelatedRoles = function (id) {
    const txRequest = TxRequestBuilder.getRelatedRoles(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
};
ResponseConverter.prototype.setRelatedRole = function (id, role) {
    const txRequest = TxRequestBuilder.setRelatedRole(id, role);
    return this.communicator.send(txRequest);
};
ResponseConverter.prototype.unsetRelatedRole = function (id, role) {
    const txRequest = TxRequestBuilder.unsetRelatedRole(id, role);
    return this.communicator.send(txRequest);
};

// Attribute type
ResponseConverter.prototype.putAttribute = function (resp) {
    const grpcConcept = resp.getConceptmethodRes().getResponse().getAttributetypeCreateRes().getConcept()
    return this.conceptFactory.createConcept(grpcConcept);
};
ResponseConverter.prototype.getAttribute = async function (id, value) {
    const dataTypeTxRequest = TxRequestBuilder.getDataTypeOfType(id);
    const resp = await this.communicator.send(dataTypeTxRequest);
    const dataType = resp.getConceptresponse().getOptionaldatatype().getPresent();
    const txRequest = TxRequestBuilder.getAttribute(id, dataType, value);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptFromOptional(response));
};
ResponseConverter.prototype.getDataTypeOfType = function (id) {
    const txRequest = TxRequestBuilder.getDataTypeOfType(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.getOptionalDataType(response));
};
ResponseConverter.prototype.getRegex = function (id) {
    const txRequest = TxRequestBuilder.getRegex(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.getOptionalRegex(response));
};
ResponseConverter.prototype.setRegex = function (id, regex) {
    const txRequest = TxRequestBuilder.setRegex(id, regex);
    return this.communicator.send(txRequest);
};

//Thing
ResponseConverter.prototype.isInferred = function (id) {
    const txRequest = TxRequestBuilder.isInferred(id);
    return this.communicator.send(txRequest)
        .then(resp => resp.getConceptresponse().getBool());
};
ResponseConverter.prototype.getDirectType = function (id) {
    const txRequest = TxRequestBuilder.getDirectType(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptFromResponse(response));
};
ResponseConverter.prototype.getRelationships = function (id) {
    const txRequest = TxRequestBuilder.getRelationships(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
}
ResponseConverter.prototype.getRelationshipsByRoles = function (id, roles) {
    const txRequest = TxRequestBuilder.getRelationshipsByRoles(id, roles);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
};
ResponseConverter.prototype.getRolesPlayedByThing = function (id) {
    const txRequest = TxRequestBuilder.getRolesPlayedByThing(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
};
ResponseConverter.prototype.getAttributes = function (id) {
    const txRequest = TxRequestBuilder.getAttributes(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
};
ResponseConverter.prototype.getAttributesByTypes = function (id, types) {
    const txRequest = TxRequestBuilder.getAttributesByTypes(id, types);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
};
ResponseConverter.prototype.getKeys = function (id) {
    const txRequest = TxRequestBuilder.getKeys(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
};
ResponseConverter.prototype.getKeysByTypes = function (id, types) {
    const txRequest = TxRequestBuilder.getKeysByTypes(id, types);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
};
ResponseConverter.prototype.setAttribute = function (id, attribute) {
    const txRequest = TxRequestBuilder.setAttribute(id, attribute);
    return this.communicator.send(txRequest);
};
ResponseConverter.prototype.unsetAttribute = function (id, attribute) {
    const txRequest = TxRequestBuilder.unsetAttribute(id, attribute);
    return this.communicator.send(txRequest);
};

// Relationship
ResponseConverter.prototype.getRolePlayers = function (id) {
    const txRequest = TxRequestBuilder.getRolePlayers(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.consumeRolePlayerIterator(response));
};
ResponseConverter.prototype.getRolePlayersByRoles = function (id, roles) {
    const txRequest = TxRequestBuilder.getRolePlayersByRoles(id, roles);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
};
ResponseConverter.prototype.setRolePlayer = function (id, role, thing) {
    const txRequest = TxRequestBuilder.setRolePlayer(id, role, thing);
    return this.communicator.send(txRequest);
};
ResponseConverter.prototype.unsetRolePlayer = function (id, role, thing) {
    const txRequest = TxRequestBuilder.unsetRolePlayer(id, role, thing);
    return this.communicator.send(txRequest);
};
// Attribute
ResponseConverter.prototype.getValue = function (resp) {
    const attrValue = resp.getConceptmethodRes().getResponse().getAttributeValueRes().getValue()
    if (attrValue.hasString()) return attrValue.getString();
    if (attrValue.hasBoolean()) return attrValue.getBoolean();
    if (attrValue.hasInteger()) return attrValue.getInteger();
    if (attrValue.hasLong()) return attrValue.getLong();
    if (attrValue.hasFloat()) return attrValue.getFloat();
    if (attrValue.hasDouble()) return attrValue.getDouble();
    if (attrValue.hasDate()) return attrValue.getDate();
};
ResponseConverter.prototype.getOwners = function (id) {
    const txRequest = TxRequestBuilder.getOwners(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
};

ResponseConverter.prototype.getDataTypeOfAttribute = function (id) {
    const txRequest = TxRequestBuilder.getDataTypeOfAttribute(id);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.dataTypeToString(response.getConceptresponse().getDatatype()));
};

// ======================= Grakn transaction methods ========================= //

ResponseConverter.prototype.getConcept = function (conceptId) {
    const txRequest = TxRequestBuilder.getConcept(conceptId);
    return this.communicator.send(txRequest)
        .then((response) => this.respConverter.conceptFromOptional(response));
}

ResponseConverter.prototype.getSchemaConcept = function (label) {
    const txRequest = TxRequestBuilder.getSchemaConcept(label);
    return this.communicator.send(txRequest)
        .then((response) => this.respConverter.conceptFromOptional(response));
}

ResponseConverter.prototype.putEntityType = function (label) {
    const txRequest = TxRequestBuilder.putEntityType(label);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptFromResponse(response));
}

ResponseConverter.prototype.putRelationshipType = function (label) {
    const txRequest = TxRequestBuilder.putRelationshipType(label);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptFromResponse(response));
}

ResponseConverter.prototype.putRole = function (label) {
    const txRequest = TxRequestBuilder.putRole(label);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptFromResponse(response));
}

ResponseConverter.prototype.putRule = function (label, when, then) {
    const txRequest = TxRequestBuilder.putRule(label, when, then);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptFromResponse(response));
}

ResponseConverter.prototype.getAttributesByValue = function (value, dataType) {
    const txRequest = TxRequestBuilder.getAttributesByValue(value, dataType);
    return this.communicator.send(txRequest)
        .then(response => this.respConverter.conceptsFromIterator(response));
}

ResponseConverter.prototype.openTx = function (keyspace, txType, credentials) {
    const txRequest = TxRequestBuilder.openTx(keyspace, txType, credentials);
    return this.communicator.send(txRequest);
};

ResponseConverter.prototype.commit = function () {
    const txRequest = TxRequestBuilder.commit();
    return this.communicator.send(txRequest);
}

ResponseConverter.prototype.execute = function executeQuery(query) {
    const txRequest = TxRequestBuilder.executeQuery(query);
    return this.communicator.send(txRequest)
        .then(resp => this.respConverter.executeResponse(resp));
};

module.exports = ResponseConverter;