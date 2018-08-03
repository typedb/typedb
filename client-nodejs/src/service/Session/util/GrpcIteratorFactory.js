/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

const RequestBuilder = require("./RequestBuilder");
const AnswerFactory = require("./AnswerFactory");

/**
 * Factory of Iterators, bound to a specific transaction
 */
function GrpcIteratorFactory(conceptFactory, communicator) {
  this.communicator = communicator;
  this.conceptFactory = conceptFactory;
  this.answerFactory = new AnswerFactory(conceptFactory);
}

// Query Iterator

GrpcIteratorFactory.prototype.createQueryIterator = function (iteratorId) {
  const mapResponse = (response) => {
    const iterRes = response.getIterateRes();
    if (iterRes.getDone()) return null;
    const answer = iterRes.getQueryIterRes().getAnswer();
    return this.answerFactory.createAnswer(answer);
  }
  const iterator = new Iterator(this.conceptFactory, this.communicator, RequestBuilder.nextReq(iteratorId), mapResponse);
  // Extend iterator with helper method collectConcepts()
  iterator.collectConcepts = async function () { return (await this.collect()).map(a => Array.from(a.map().values())).reduce((a, c) => a.concat(c), []); };
  return iterator;
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

  const mapResponse = (response, conceptFactory) => {
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
