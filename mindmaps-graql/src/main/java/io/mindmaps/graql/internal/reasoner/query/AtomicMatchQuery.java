package io.mindmaps.graql.internal.reasoner.query;

import com.google.common.collect.Sets;
import io.mindmaps.concept.Concept;
import io.mindmaps.graql.internal.reasoner.predicate.Atomic;

import java.util.Map;
import java.util.stream.Stream;

import static io.mindmaps.graql.internal.reasoner.query.QueryAnswers.getUnifiedAnswers;

public class AtomicMatchQuery extends AtomicQuery{

    final private QueryAnswers answers;

    public AtomicMatchQuery(Atomic atom){
        super(atom);
        answers = new QueryAnswers();
    }

    public AtomicMatchQuery(AtomicQuery query, QueryAnswers ans){
        super(query);
        answers = new QueryAnswers(ans);
    }

    public AtomicMatchQuery(AtomicMatchQuery query){
        super(query);
        answers = new QueryAnswers(query.getAnswers());
    }

    @Override
    public Stream<Map<String, Concept>> stream() {
        return answers.stream();
    }

    @Override
    public QueryAnswers getAnswers(){ return answers;}

    @Override
    public void DBlookup() {
        answers.addAll(Sets.newHashSet(getMatchQuery().distinct()));
    }

    @Override
    public void memoryLookup(Map<AtomicQuery, AtomicQuery> matAnswers) {
        AtomicQuery equivalentQuery = matAnswers.get(this);
        if(equivalentQuery != null) {
            QueryAnswers unifiedAnswers = getUnifiedAnswers(this, equivalentQuery, equivalentQuery.getAnswers());
            answers.addAll(unifiedAnswers);
        }
    }

    @Override
    public void propagateAnswers(Map<AtomicQuery, AtomicQuery> matAnswers) {
        getChildren().forEach(childQuery -> {
            QueryAnswers ans = getUnifiedAnswers(childQuery, this, matAnswers.get(this).getAnswers());
            childQuery.getAnswers().addAll(ans);
            childQuery.propagateAnswers(matAnswers);
        });
    }

}
