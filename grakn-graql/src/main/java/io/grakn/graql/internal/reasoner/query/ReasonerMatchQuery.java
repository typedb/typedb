package io.grakn.graql.internal.reasoner.query;


import io.grakn.MindmapsGraph;
import io.grakn.concept.Concept;
import io.grakn.graql.MatchQuery;

import java.util.Map;
import java.util.stream.Stream;

public class ReasonerMatchQuery extends Query{

    final private QueryAnswers answers;

    public ReasonerMatchQuery(MatchQuery query, MindmapsGraph graph){
        super(query, graph);
        answers = new QueryAnswers();
    }

    public ReasonerMatchQuery(MatchQuery query, MindmapsGraph graph, QueryAnswers ans){
        super(query, graph);
        answers = new QueryAnswers(ans);
    }

    @Override
    public Stream<Map<String, Concept>> stream() {
        return answers.stream();
    }
}
