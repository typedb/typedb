package grakn.core.graql.reasoner.cache;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.atom.predicate.IdPredicate;
import grakn.core.graql.reasoner.query.ReasonerAtomicQuery;
import java.util.HashSet;
import java.util.Set;

public abstract class AtomicQueryCacheBase<
        QE,
        SE extends Set<ConceptMap>> extends QueryCacheBase<ReasonerAtomicQuery, Set<ConceptMap>, QE, SE> {

    final private Set<ReasonerAtomicQuery> dbCompleteQueries = new HashSet<>();
    final private Set<QE> dbCompleteEntries = new HashSet<>();

    final private Set<ReasonerAtomicQuery> completeQueries = new HashSet<>();
    final private Set<QE> completeEntries = new HashSet<>();

    boolean isDBComplete(ReasonerAtomicQuery query){
        return dbCompleteEntries.contains(queryToKey(query))
                || dbCompleteQueries.contains(query);
    }

    public boolean isComplete(ReasonerAtomicQuery query){
        return completeEntries.contains(queryToKey(query))
                || completeQueries.contains(query);
    }

    public void ackCompleteness(ReasonerAtomicQuery query) {
        if (query.getAtom().getPredicates(IdPredicate.class).findFirst().isPresent()) {
            completeQueries.add(query);
        } else {
            completeEntries.add(queryToKey(query));
        }
    }

    public void ackDBCompleteness(ReasonerAtomicQuery query){
        if (query.getAtom().getPredicates(IdPredicate.class).findFirst().isPresent()) {
            dbCompleteQueries.add(query);
        } else {
            dbCompleteEntries.add(queryToKey(query));
        }
    }

    void ackCompletenessFromParent(ReasonerAtomicQuery query, ReasonerAtomicQuery parent){
        if (completeQueries.contains(parent)) completeQueries.add(query);
        if (completeEntries.contains(queryToKey(parent))){
            completeEntries.add(queryToKey(query));
        }
    }

    void ackDBCompletenessFromParent(ReasonerAtomicQuery query, ReasonerAtomicQuery parent){
        if (dbCompleteQueries.contains(parent)) dbCompleteQueries.add(query);
        if (dbCompleteEntries.contains(queryToKey(parent))){
            dbCompleteEntries.add(queryToKey(query));
        }
    }
}
