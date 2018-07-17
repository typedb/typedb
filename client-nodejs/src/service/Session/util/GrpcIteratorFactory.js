const RequestBuilder = require("./RequestBuilder");

function GrpcIteratorFactory(conceptFactory, communicator) {
  this.communicator = communicator;
  this.conceptFactory = conceptFactory;
}

// Query Iterator

GrpcIteratorFactory.prototype.createQueryIterator = function (iteratorId) {
  function mapQueryAnswer(queryAnswer, conceptFactory) {
    const answerMap = new Map();
    queryAnswer.getQueryanswerMap()
      .forEach((grpcConcept, key) => {
        answerMap.set(key, conceptFactory.createConcept(grpcConcept));
      });
    return answerMap;
  }

  function mapComputeAnswer(computeAnswer, conceptFactory) {

  }

  mapResponse = (response, conceptFactory) => {
    const iterRes = response.getIterateRes();
    if (iterRes.getDone()) return null;
    const answer = iterRes.getQueryIterRes().getAnswer();
    if (answer.hasQueryanswer()) return mapQueryAnswer(answer.getQueryanswer(), conceptFactory);
    if (answer.hasComputeanswer()) return mapComputeAnswer(answer.getComputeAnswer(), conceptFactory);
    // add aggregate answer
  }
  return new Iterator(this.conceptFactory, this.communicator, RequestBuilder.nextReq(iteratorId), mapResponse);
};

//Concept Iterator
GrpcIteratorFactory.prototype.createConceptIterator = function (iteratorId, getterMethod) {
  const mapResponse = (response, conceptFactory) => {
    const iterRes = response.getIterateRes();
    if (iterRes.getDone()) return null;
    const grpcConcept = getterMethod(iterRes);
    return conceptFactory.createConcept(grpcConcept);
  }
  return new Iterator(this.conceptFactory, this.communicator, RequestBuilder.nextReq(iteratorId), mapResponse);
};


// Role player Iterator
GrpcIteratorFactory.prototype.createRolePlayerIterator = function (iteratorId) {

  mapResponse = (response, conceptFactory) => {
    const iterRes = response.getIterateRes();
    if (iterRes.getDone()) return null;
    const resContent = iterRes.getConceptmethodIterRes().getRelationRoleplayersmapIterRes();
    return {
      role: conceptFactory.createConcept(resContent.getRole()),
      player: conceptFactory.createConcept(resContent.getPlayer())
    };
  }
  return new Iterator(this.conceptFactory, this.communicator, RequestBuilder.nextReq(iteratorId), mapResponse);
};

//Iterator 

function Iterator(conceptFactory, communicator, nextRequest, mapResponse) {
  this.next = () => {
    return communicator.send(nextRequest)
      .then((resp) => mapResponse(resp, conceptFactory))
      .catch(e => { throw e; });
  }

  this.collect = async () => {
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
