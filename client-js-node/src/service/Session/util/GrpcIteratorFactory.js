const TxRequestBuilder = require("./TxRequestBuilder");

function GrpcIteratorFactory(communicator) {
  this.communicator = communicator;
}

GrpcIteratorFactory.prototype.createQueryIterator = function (iteratorId) {
  return new GrpcQueryIterator(this.communicator, TxRequestBuilder.next(iteratorId));
};
GrpcIteratorFactory.prototype.createConceptIterator = function (iteratorId) {
  return new GrpcConceptIterator(this.communicator, TxRequestBuilder.next(iteratorId));
};
GrpcIteratorFactory.prototype.createRolePlayerIterator = function (iteratorId) {
  return new GrpcRolePlayerIterator(this.communicator, TxRequestBuilder.next(iteratorId));
};

// -- Query Iterator -- // 

function GrpcQueryIterator(communicator, nextRequest) {
  this.nextRequest = nextRequest;
  this.communicator = communicator;
};

GrpcQueryIterator.prototype.nextResult = async function () {
  return await this.communicator.send(this.nextRequest)
    .then(handleQueryResponse)
    .catch(e => { throw e; });
};

function handleQueryResponse(response) {
  if (response.hasDone()) return null;
  if (response.hasQueryresult()) return response.getQueryresult();
  throw "Unexpected response from server.";
}

// -- Concept Iterator -- //

function GrpcConceptIterator(communicator, nextRequest) {
  this.nextRequest = nextRequest;
  this.communicator = communicator;
}

GrpcConceptIterator.prototype.nextResult = async function () {
  return await this.communicator.send(this.nextRequest)
    .then(handleConceptResponse)
    .catch(e => { throw e; });
};

function handleConceptResponse(response) {
  if (response.hasDone()) return null;
  if (response.hasConcept()) return response.getConcept();
  throw "Unexpected response from server.";
}

// -- RolePlayer Iterator -- //

function GrpcRolePlayerIterator(communicator, nextRequest) {
  this.nextRequest = nextRequest;
  this.communicator = communicator;
}

GrpcRolePlayerIterator.prototype.nextResult = async function () {
  return await this.communicator.send(this.nextRequest)
    .then(handleRolePlayerResponse)
    .catch(e => { throw e; });
};

function handleRolePlayerResponse(response) {
  if (response.hasDone()) return null;
  if (response.hasRoleplayer()) return response.getRoleplayer();
  throw "Unexpected response from server.";
}

module.exports = GrpcIteratorFactory;
