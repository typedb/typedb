const GrpcIteratorFactory = require("./GrpcIteratorFactory");

function ResponseConverter(conceptFactory, communicator) {
    this.iteratorFactory = new GrpcIteratorFactory(conceptFactory, communicator);
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

ResponseConverter.prototype.putAttributeType = function (resp) {
    const concept = resp.getPutattributetypeRes().getConcept();
    return this.conceptFactory.createConcept(concept);
}


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


module.exports = ResponseConverter;