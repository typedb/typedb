const TxRequestBuilder = require("./TxRequestBuilder");

function GrpcIteratorFactory(conceptFactory, communicator) {
  this.communicator = communicator;
  this.conceptFactory = conceptFactory;
}

GrpcIteratorFactory.prototype.createQueryIterator = function (iteratorId) {
  return new GrpcQueryIterator(this.communicator, TxRequestBuilder.next(iteratorId));
};
GrpcIteratorFactory.prototype.createConceptIterator = function (iteratorId, method) {
  return new GrpcConceptIterator(this.conceptFactory, this.communicator, TxRequestBuilder.nextReq(iteratorId), method);
};
GrpcIteratorFactory.prototype.createRolePlayerIterator = function (iteratorId) {
  return new GrpcRolePlayerIterator(this.communicator, TxRequestBuilder.next(iteratorId));
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

  handleConceptResponse = (response) => {
    const iterRes = response.getIterateRes();
    if (iterRes.getDone()) return null;
    debugger;
    return iterRes.getConceptmethodIterRes()[getterMethod]().getConcept();
  }

  this.next = () => {
    return communicator.send(nextRequest)
      .then(handleConceptResponse)
      .catch(e => { throw e; });
  }

  this.collectAll = async () => {
    const concepts = [];
    let concept = await this.next();
    while (concept) {
      concepts.push(conceptFactory.createConcept(concept));
      concept = await this.next();
    }
    return concepts;
  }
}

// -- RolePlayer Iterator -- //

function GrpcRolePlayerIterator(communicator, nextRequest) {
  this.nextRequest = nextRequest;
  this.communicator = communicator;
}

GrpcRolePlayerIterator.prototype.next = function () {
  return this.communicator.send(this.nextRequest)
    .then(handleRolePlayerResponse)
    .catch(e => { throw e; });
};

function handleRolePlayerResponse(response) {
  if (response.hasDone()) return null;
  if (response.hasRoleplayer()) return response.getRoleplayer();
  throw "Unexpected response from server.";
}

module.exports = GrpcIteratorFactory;
