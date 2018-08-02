/**
 * Factory for Answer and Explanation objects
 * @param {Object} conceptFactory 
 */
function AnswerFactory(conceptFactory) {
    this.conceptFactory = conceptFactory;
}

AnswerFactory.prototype.createAnswer = function (grpcAnswer) {
    if (grpcAnswer.hasConceptmap()) return this.createConceptmap(grpcAnswer.getConceptmap());
    if (grpcAnswer.hasAnswergroup()) return this.createAnswergroup(grpcAnswer.getAnswergroup());
    if (grpcAnswer.hasConceptlist()) return this.createConceptlist(grpcAnswer.getConceptlist());
    if (grpcAnswer.hasConceptset()) return this.createConceptset(grpcAnswer.getConceptset());
    if (grpcAnswer.hasConceptsetmeasure()) return this.createConceptsetmeasure(grpcAnswer.getConceptsetmeasure());
    if (grpcAnswer.hasValue()) return this.createValue(grpcAnswer.getValue());
}

AnswerFactory.prototype.buildExplanation = function (grpcExplanation) {
    if (!grpcExplanation) return null;
    return {
        queryPattern: () => grpcExplanation.getQuerypattern(),
        answers: () => grpcExplanation.getAnswersList().map(a => this.createConceptmap(a))
    }
}

AnswerFactory.prototype.createConceptmap = function (answer) {
    const answerMap = new Map();
    answer.getMapMap().forEach((grpcConcept, key) => {
            answerMap.set(key, this.conceptFactory.createConcept(grpcConcept));
        });
    return {
        map: () => answerMap,
        explanation: () => this.buildExplanation(answer.getExplanation())
    }
}
AnswerFactory.prototype.createValue = function(answer){
    return {
        number: () => Number(answer.getNumber().getValue()),
        explanation: () => this.buildExplanation(answer.getExplanation())
    }
}
AnswerFactory.prototype.createConceptlist = function(answer){
    const list = answer.getList();
    return {
        list: () => list.getIdsList(),
        explanation: () => this.buildExplanation(answer.getExplanation())
    }   
}
AnswerFactory.prototype.createConceptset = function(answer){
    const set = answer.getSet();
    return {
        set: () => new Set(set.getIdsList()),
        explanation: () => this.buildExplanation(answer.getExplanation())
    }
}
AnswerFactory.prototype.createConceptsetmeasure = function(answer){
    return {
        measurement: () => Number(answer.getMeasurement().getValue()),
        set: () => new Set(answer.getSet().getIdsList()),
        explanation: () => this.buildExplanation(answer.getExplanation())
    }
}
AnswerFactory.prototype.createAnswergroup = function(answer){
    return {
        owner: () => this.conceptFactory.createConcept(answer.getOwner()),
        answers: () => answer.getAnswersList().map(a => this.createAnswer(a)),
        explanation: () => this.buildExplanation(answer.getExplanation())
    }
}

module.exports = AnswerFactory;