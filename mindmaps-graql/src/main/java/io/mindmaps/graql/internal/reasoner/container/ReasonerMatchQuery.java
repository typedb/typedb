package io.mindmaps.graql.internal.reasoner.container;


import io.mindmaps.concept.Concept;

import java.util.Map;
import java.util.stream.Stream;

public class ReasonerMatchQuery extends Query{

    final private QueryAnswers answers;

    public ReasonerMatchQuery(Query query, QueryAnswers ans){
        super(query);
        answers = new QueryAnswers(ans);
    }

    @Override
    public Stream<Map<String, Concept>> stream() {
        return answers.stream();
    }
}
