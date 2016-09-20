package io.mindmaps.graql.internal.reasoner.query;


import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Concept;
import io.mindmaps.graql.MatchQuery;

import java.util.Map;
import java.util.stream.Stream;

public class ReasonerMatchQuery extends Query{

    final private QueryAnswers answers;

    public ReasonerMatchQuery(MatchQuery query, MindmapsGraph graph){
        super(query, graph);
        answers = new QueryAnswers();
    }

    public ReasonerMatchQuery(MatchQuery query, QueryAnswers ans, MindmapsGraph graph){
        super(query, graph);
        answers = new QueryAnswers(ans);
    }

    public ReasonerMatchQuery(Query query, QueryAnswers ans){
        super(query);
        answers = new QueryAnswers(ans);
    }

    @Override
    public Stream<Map<String, Concept>> stream() {
        return answers.stream();
    }
}
