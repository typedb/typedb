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

ResponseConverter.prototype.getAttributeValueFromResponse = function (resp) {
    const attrValue = resp.getConceptresponse().getAttributevalue();
    if (attrValue.hasString()) return attrValue.getString();
    if (attrValue.hasBoolean()) return attrValue.getBoolean();
    if (attrValue.hasInteger()) return attrValue.getInteger();
    if (attrValue.hasLong()) return attrValue.getLong();
    if (attrValue.hasFloat()) return attrValue.getFloat();
    if (attrValue.hasDouble()) return attrValue.getDouble();
    if (attrValue.hasDate()) return attrValue.getDate();
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


module.exports = ResponseConverter;