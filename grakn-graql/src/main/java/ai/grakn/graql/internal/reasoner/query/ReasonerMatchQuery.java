package ai.grakn.graql.internal.reasoner.query;


import ai.grakn.concept.Concept;
import ai.grakn.graql.MatchQuery;
import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.graql.MatchQuery;

import java.util.Map;
import java.util.stream.Stream;

public class ReasonerMatchQuery extends Query{

    final private QueryAnswers answers;

    public ReasonerMatchQuery(MatchQuery query, GraknGraph graph){
        super(query, graph);
        answers = new QueryAnswers();
    }

    public ReasonerMatchQuery(MatchQuery query, GraknGraph graph, QueryAnswers ans){
        super(query, graph);
        answers = new QueryAnswers(ans);
    }

    @Override
    public Stream<Map<String, Concept>> stream() {
        return answers.stream();
    }
}
