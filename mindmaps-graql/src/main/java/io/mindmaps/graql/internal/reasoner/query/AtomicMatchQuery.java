package io.mindmaps.graql.internal.reasoner.query;

import com.google.common.collect.Sets;
import io.mindmaps.concept.Concept;
import io.mindmaps.graql.internal.reasoner.predicate.Atomic;

import io.mindmaps.graql.internal.reasoner.predicate.Substitution;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static io.mindmaps.graql.internal.reasoner.Utility.findEquivalentAtomicQuery;
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
    public void memoryLookup(Map<AtomicQuery, QueryAnswers> matAnswers) {
        QueryAnswers memAnswers = new QueryAnswers();

        if (matAnswers.keySet().contains(this)) {
            AtomicQuery equivalentQuery = findEquivalentAtomicQuery(this, matAnswers.keySet());
            memAnswers = getUnifiedAnswers(this, equivalentQuery, matAnswers.get(equivalentQuery));
        }
        answers.addAll(memAnswers);
    }

    @Override
    public void propagateAnswers(Map<AtomicQuery, QueryAnswers> matAnswers) {
        getChildren().forEach(childQuery -> {
            QueryAnswers ans = getUnifiedAnswers(childQuery, this, matAnswers.get(this));
            childQuery.getAnswers().addAll(ans);
            childQuery.propagateAnswers(matAnswers);
        });
    }

    @Override
    public QueryAnswers materialise(){
        QueryAnswers fullAnswers = new QueryAnswers();
        answers.forEach(answer -> {
            Set<Substitution> subs = new HashSet<>();
            answer.forEach((var, con) -> {
                Substitution sub = new Substitution(var, con);
                if (!containsAtom(sub))
                    subs.add(sub);
            });
            fullAnswers.addAll(materialise(subs));
        });
        return fullAnswers;
    }
}
