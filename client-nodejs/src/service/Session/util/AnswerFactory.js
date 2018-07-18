

/**
 * Builds 
 * @param {Object} conceptFactory 
 */
function AnswerFactory(conceptFactory) {
    this.conceptFactory = conceptFactory;
}

AnswerFactory.prototype.createAnswer = function (grpcAnswer) {
    if (grpcAnswer.hasQueryanswer()) return this.createQueryAnswer(grpcAnswer.getQueryanswer());
}

AnswerFactory.prototype.buildExplanation = function (grpcExplanation) {
    if (!grpcExplanation) return null;
    const answersArray = grpcExplanation.getQueryanswerList().map(a => this.createQueryAnswer(a));

    return {
        queryPattern: () => grpcExplanation.getQuerypattern(),
        answers: () => answersArray
    }
}

AnswerFactory.prototype.createQueryAnswer = function (answer) {
    const answerMap = new Map();
    answer.getQueryanswerMap()
        .forEach((grpcConcept, key) => {
            answerMap.set(key, this.conceptFactory.createConcept(grpcConcept));
        });
    const explanation = this.buildExplanation(answer.getExplanation());

    return {
        get: () => answerMap,
        explanation: () => explanation
    }
}


module.exports = AnswerFactory;