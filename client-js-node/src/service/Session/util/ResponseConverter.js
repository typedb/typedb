const GrpcIteratorFactory = require("./GrpcIteratorFactory");

function ResponseConverter(conceptFactory, communicator) {
    this.iteratorFactory = new GrpcIteratorFactory(conceptFactory, communicator);
    this.conceptFactory = conceptFactory;
}

function dataTypeToString(dataType) {
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

// Entity type
ResponseConverter.prototype.addEntity = function (resp) {
    const grpcConcept = resp.getConceptmethodRes().getResponse().getEntitytypeCreateRes().getConcept();
    return this.conceptFactory.createConcept(grpcConcept);
};

// Attribute type
ResponseConverter.prototype.getAttribute = function (resp) {
    const grpcRes = resp.getConceptmethodRes().getResponse().getAttributetypeAttributeRes();
    return (grpcRes.hasNull()) ? null : this.conceptFactory.createConcept(grpcRes.getConcept());
}
ResponseConverter.prototype.putAttribute = function (resp) {
    const grpcConcept = resp.getConceptmethodRes().getResponse().getAttributetypeCreateRes().getConcept();
    return this.conceptFactory.createConcept(grpcConcept);
};

ResponseConverter.prototype.getDataTypeOfType = function (resp) {
    const dataType = resp.getConceptmethodRes().getResponse().getAttributetypeDatatypeRes().getDatatype();
    return dataTypeToString(dataType);
}

ResponseConverter.prototype.getRegex = function (resp) {
    return resp.getConceptmethodRes().getResponse().getAttributetypeGetregexRes().getRegex();
}

//Relation type
ResponseConverter.prototype.addRelationship = function (resp) {
    const grpcConcept = resp.getConceptmethodRes().getResponse().getRelationtypeCreateRes().getConcept();
    return this.conceptFactory.createConcept(grpcConcept);
};

ResponseConverter.prototype.getRelatedRoles = function (resp) {
    const iterId = resp.getConceptmethodRes().getResponse().getRelationtypeRolesIter().getId();
    const getterMethod = "getRelationtypeRolesIterRes";
    return this.iteratorFactory.createConceptIterator(iterId, getterMethod);
}

//Thing
ResponseConverter.prototype.isInferred = function (resp) {
    return resp.getConceptmethodRes().getResponse().getThingIsinferredRes().getInferred();
}

ResponseConverter.prototype.getDirectType = function (resp) {
    const grpcConcept = resp.getConceptmethodRes().getResponse().getThingTypeRes().getConcept();
    return this.conceptFactory.createConcept(grpcConcept);
}

ResponseConverter.prototype.getRelationshipsByRoles = function (resp) {
    const iterId = resp.getConceptmethodRes().getResponse().getThingRelationsIter().getId();
    const getterMethod = "getThingRelationsIterRes";
    return this.iteratorFactory.createConceptIterator(iterId, getterMethod);
}
ResponseConverter.prototype.getRolesPlayedByThing = function (resp) {
    const iterId = resp.getConceptmethodRes().getResponse().getThingRolesIter().getId();
    const getterMethod = "getThingRolesIterRes";
    return this.iteratorFactory.createConceptIterator(iterId, getterMethod);
}
ResponseConverter.prototype.getAttributesByTypes = function (resp) {
    const iterId = resp.getConceptmethodRes().getResponse().getThingAttributesIter().getId();
    const getterMethod = "getThingAttributesIterRes";
    return this.iteratorFactory.createConceptIterator(iterId, getterMethod);
}
ResponseConverter.prototype.getKeysByTypes = function (resp) {
    const iterId = resp.getConceptmethodRes().getResponse().getThingKeysIter().getId();
    const getterMethod = "getThingKeysIterRes";
    return this.iteratorFactory.createConceptIterator(iterId, getterMethod);
}


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
ResponseConverter.prototype.getOwners = function (resp) {
    const getterMethod = "getAttributeOwnersIterRes";
    const iterId = resp.getConceptmethodRes().getResponse().getAttributeOwnersIter().getId();
    return this.iteratorFactory.createConceptIterator(iterId, getterMethod);
}

//Relation

ResponseConverter.prototype.rolePlayersMap = async function (resp) {
    const iterId = resp.getConceptmethodRes().getResponse().getRelationRoleplayersmapIter().getId();
    const getterMethod = "getRelationRoleplayersmapIterRes";
    const iterator = this.iteratorFactory.createRolePlayerIterator(iterId, getterMethod);
    const rolePlayers = await iterator.collectAll();
    // Temp map to store String id to Role object
    const tempMap = new Map(rolePlayers.map(entry => [entry.role.id, entry.role]));
    const map = new Map();
    // Create map using string as key and set as value
    rolePlayers.forEach(rp => {
        const key = rp.role.id;
        if (map.has(key)) map.set(key, map.get(key).add(rp.player));
        else map.set(key, new Set([rp.player]));
    })
    const resultMap = new Map();
    // Convert map to use Role object as key
    map.forEach((value, key) => {
        resultMap.set(tempMap.get(key), value);
    });
    return resultMap;
}

ResponseConverter.prototype.rolePlayers = function (resp) {
    const iterId = resp.getConceptmethodRes().getResponse().getRelationRoleplayersIter().getId();
    const getterMethod = "getRelationRoleplayersIterRes";
    return this.iteratorFactory.createConceptIterator(iterId, getterMethod);
}

// Rule

ResponseConverter.prototype.getWhen = function (resp) {
    const methodRes = resp.getConceptmethodRes().getResponse().getRuleWhenRes();
    return (methodRes.hasNull()) ? null : methodRes.getPattern();
}

ResponseConverter.prototype.getThen = function (resp) {
    const methodRes = resp.getConceptmethodRes().getResponse().getRuleThenRes();
    return (methodRes.hasNull()) ? null : methodRes.getPattern();
}


// ======================= Grakn transaction methods ========================= //

ResponseConverter.prototype.getSchemaConcept = function (resp) {
    const grpcRes = resp.getGetschemaconceptRes();
    return (grpcRes.hasNull()) ? null : this.conceptFactory.createConcept(grpcRes.getConcept());
}

ResponseConverter.prototype.getConcept = function (resp) {
    const grpcRes = resp.getGetconceptRes();
    return (grpcRes.hasNull()) ? null : this.conceptFactory.createConcept(grpcRes.getConcept());
}

ResponseConverter.prototype.putEntityType = function (resp) {
    const concept = resp.getPutentitytypeRes().getConcept();
    return this.conceptFactory.createConcept(concept);
}

ResponseConverter.prototype.putRelationshipType = function (resp) {
    const concept = resp.getPutrelationshiptypeRes().getConcept();
    return this.conceptFactory.createConcept(concept);
}

ResponseConverter.prototype.putAttributeType = function (resp) {
    const concept = resp.getPutattributetypeRes().getConcept();
    return this.conceptFactory.createConcept(concept);
}

ResponseConverter.prototype.putRole = function (resp) {
    const concept = resp.getPutroleRes().getConcept();
    return this.conceptFactory.createConcept(concept);
}

ResponseConverter.prototype.putRule = function (resp) {
    const concept = resp.getPutruleRes().getConcept();
    return this.conceptFactory.createConcept(concept);
}

module.exports = ResponseConverter;