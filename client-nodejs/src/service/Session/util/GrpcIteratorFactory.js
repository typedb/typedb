const TxRequestBuilder = require("./TxRequestBuilder");

function GrpcIteratorFactory(conceptFactory, communicator) {
  this.communicator = communicator;
  this.conceptFactory = conceptFactory;
}

GrpcIteratorFactory.prototype.createQueryIterator = function (iteratorId) {
  return new GrpcQueryIterator(this.conceptFactory, this.communicator, TxRequestBuilder.nextReq(iteratorId));
};
GrpcIteratorFactory.prototype.createAttributesIterator = function (iteratorId) {
  return new AttributesIterator(this.conceptFactory, this.communicator, TxRequestBuilder.nextReq(iteratorId));
};
GrpcIteratorFactory.prototype.createConceptIterator = function (iteratorId, method) {
  return new GrpcConceptIterator(this.conceptFactory, this.communicator, TxRequestBuilder.nextReq(iteratorId), method);
};
GrpcIteratorFactory.prototype.createRolePlayerIterator = function (iteratorId, method) {
  return new GrpcRolePlayerIterator(this.conceptFactory, this.communicator, TxRequestBuilder.nextReq(iteratorId), method);
};

// -- Query Iterator -- // 

function GrpcQueryIterator(conceptFactory, communicator, nextRequest) {

  function mapQueryAnswer(queryAnswer) {
    const answerMap = new Map();
    queryAnswer.getQueryanswerMap()
      .forEach((grpcConcept, key) => {
        answerMap.set(key, conceptFactory.createConcept(grpcConcept));
      });
    return answerMap;
  }

  function mapComputeAnswer(computeAnswer) {

  }

  mapResponse = (response) => {
    const iterRes = response.getIterateRes();
    if (iterRes.getDone()) return null;
    const answer = iterRes.getQueryIterRes().getAnswer();
    if (answer.hasQueryanswer()) return mapQueryAnswer(answer.getQueryanswer());
    if (answer.hasComputeanswer()) return mapComputeAnswer(answer.getComputeAnswer());
    // add aggregate answer
  }

  this.next = () => {
    return communicator.send(nextRequest)
      .then(mapResponse)
      .catch(e => { throw e; });
  }

  this.collectAll = async () => {
    const results = [];
    let result = await this.next();
    while (result) {
      results.push(result);
      result = await this.next();
    }
    return results;
  }
}


// -- Concept Iterator -- //

function GrpcConceptIterator(conceptFactory, communicator, nextRequest, getterMethod) {

  mapResponse = (response) => {
    const iterRes = response.getIterateRes();
    if (iterRes.getDone()) return null;
    const grpcConcept = getterMethod(iterRes);
    return conceptFactory.createConcept(grpcConcept);
  }

  this.next = () => {
    return communicator.send(nextRequest)
      .then(mapResponse)
      .catch(e => { throw e; });
  }

  this.collectAll = async () => {
    const results = [];
    let result = await this.next();
    while (result) {
      results.push(result);
      result = await this.next();
    }
    return results;
  }
}

// -- RolePlayer Iterator -- //

function GrpcRolePlayerIterator(conceptFactory, communicator, nextRequest, getterMethod) {

  mapResponse = (response) => {
    const iterRes = response.getIterateRes();
    if (iterRes.getDone()) return null;
    const resContent = iterRes.getConceptmethodIterRes()[getterMethod]();
    return {
      role: conceptFactory.createConcept(resContent.getRole()),
      player: conceptFactory.createConcept(resContent.getPlayer())
    };
  }

  this.next = () => {
    return communicator.send(nextRequest)
      .then(mapResponse)
      .catch(e => { throw e; });
  }

  this.collectAll = async () => {
    const results = [];
    let result = await this.next();
    while (result) {
      results.push(result);
      result = await this.next();
    }
    return results;
  }
}

// -- Attributes Iterator -- //

function AttributesIterator(conceptFactory, communicator, nextRequest) {

  mapResponse = (response) => {
    const iterRes = response.getIterateRes();
    if (iterRes.getDone()) return null;
    const grpcConcept = iterRes.getGetattributesIterRes().getAttribute();
    return conceptFactory.createConcept(grpcConcept);
  }

  this.next = () => {
    return communicator.send(nextRequest)
      .then(mapResponse)
      .catch(e => { throw e; });
  }

  this.collectAll = async () => {
    const results = [];
    let result = await this.next();
    while (result) {
      results.push(result);
      result = await this.next();
    }
    return results;
  }
}

module.exports = GrpcIteratorFactory;
