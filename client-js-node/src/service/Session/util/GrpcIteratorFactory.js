const TxRequestBuilder = require("./TxRequestBuilder");

function GrpcIteratorFactory(conceptFactory, communicator) {
  this.communicator = communicator;
  this.conceptFactory = conceptFactory;
}

GrpcIteratorFactory.prototype.createQueryIterator = function (iteratorId, method) {
  return new GrpcQueryIterator(this.conceptFactory, this.communicator, TxRequestBuilder.nextReq(iteratorId), method);
};
GrpcIteratorFactory.prototype.createConceptIterator = function (iteratorId, method) {
  return new GrpcConceptIterator(this.conceptFactory, this.communicator, TxRequestBuilder.nextReq(iteratorId), method);
};
GrpcIteratorFactory.prototype.createRolePlayerIterator = function (iteratorId, method) {
  return new GrpcRolePlayerIterator(this.conceptFactory, this.communicator, TxRequestBuilder.nextReq(iteratorId), method);
};

// -- Query Iterator -- // 

function GrpcQueryIterator(communicator, nextRequest) {
  this.nextRequest = nextRequest;
  this.communicator = communicator;
};

GrpcQueryIterator.prototype.next = function () {
  return this.communicator.send(this.nextRequest)
    .then(handleQueryResponse)
    .catch(e => { throw e; });
};

function handleQueryResponse(response) {
  if (response.hasDone()) return null;
  if (response.hasQueryresult()) return response.getQueryresult();
  throw "Unexpected response from server.";
}

// -- Concept Iterator -- //

function GrpcConceptIterator(conceptFactory, communicator, nextRequest, getterMethod) {

  mapResponse = (response) => {
    const iterRes = response.getIterateRes();
    if (iterRes.getDone()) return null;
    const grpcConcept = iterRes.getConceptmethodIterRes()[getterMethod]().getConcept();
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

module.exports = GrpcIteratorFactory;
