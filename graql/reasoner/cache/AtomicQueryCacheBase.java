/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.graql.reasoner.cache;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.atom.predicate.IdPredicate;
import grakn.core.graql.reasoner.query.ReasonerAtomicQuery;
import grakn.core.kb.graql.executor.ExecutorFactory;
import grakn.core.kb.graql.executor.TraversalExecutor;
import grakn.core.kb.graql.planning.gremlin.TraversalPlanFactory;

import java.util.HashSet;
import java.util.Set;

/**
 *
 * Base class for query caches for atomic queries.
 * Implements the acking behaviour for queries:
 *
 * - DB completeness - a query is marked as DB complete if we are sure that the cache contains all the DB (persisted)
 * answers to that query
 *
 * - completeness - a query is marked as complete if we are sure that the cache contains all the answers (including inferred)
 * to that query. A complete query is also DB complete.
 *
 * @param <QE> cache entry query type
 * @param <SE> cache entry storage type
 *
 */
public abstract class AtomicQueryCacheBase<
        QE,
        SE extends Set<ConceptMap>> extends QueryCacheBase<ReasonerAtomicQuery, Set<ConceptMap>, QE, SE> {

    final private Set<ReasonerAtomicQuery> dbCompleteQueries = new HashSet<>();
    final private Set<QE> dbCompleteEntries = new HashSet<>();

    final private Set<ReasonerAtomicQuery> completeQueries = new HashSet<>();
    final private Set<QE> completeEntries = new HashSet<>();

    AtomicQueryCacheBase(TraversalPlanFactory traversalPlanFactory, TraversalExecutor traversalExecutor) {
        super(traversalPlanFactory, traversalExecutor);
    }

    public boolean isDBComplete(ReasonerAtomicQuery query){
        return dbCompleteEntries.contains(queryToKey(query))
                || dbCompleteQueries.contains(query);
    }

    public boolean isComplete(ReasonerAtomicQuery query){
        return completeEntries.contains(queryToKey(query))
                || completeQueries.contains(query);
    }

    public void ackCompleteness(ReasonerAtomicQuery query) {
        ackDBCompleteness(query);
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

    void unackCompleteness(ReasonerAtomicQuery query) {
        unackDBCompleteness(query);
        if (query.getAtom().getPredicates(IdPredicate.class).findFirst().isPresent()) {
            completeQueries.remove(query);
        } else {
            completeEntries.remove(queryToKey(query));
        }
    }

    private void unackDBCompleteness(ReasonerAtomicQuery query){
        if (query.getAtom().getPredicates(IdPredicate.class).findFirst().isPresent()) {
            dbCompleteQueries.remove(query);
        } else {
            dbCompleteEntries.remove(queryToKey(query));
        }
    }

    void clearQueryCompleteness(){
        dbCompleteQueries.clear();
        completeQueries.clear();
    }

    void clearCompleteness(){
        dbCompleteQueries.clear();
        dbCompleteEntries.clear();

        completeQueries.clear();
        completeEntries.clear();
    }

    @Override
    public void clear(){
        super.clear();
        clearCompleteness();
    }
}
