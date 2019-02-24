/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package grakn.core.graql.internal.reasoner.cache;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Sets;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.concept.SchemaConcept;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.reasoner.atom.predicate.IdPredicate;
import grakn.core.graql.internal.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.internal.reasoner.query.ReasonerQueries;
import grakn.core.graql.internal.reasoner.unifier.MultiUnifier;
import grakn.core.graql.internal.reasoner.unifier.MultiUnifierImpl;
import grakn.core.graql.internal.reasoner.unifier.UnifierType;
import grakn.core.graql.internal.reasoner.utils.Pair;
import graql.lang.statement.Variable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;


/**
 * Base implementation of a semantic query cache - query cache capable of recognising relations between queries
 * and subsequently reusing their answer sets.
 *
 * Relies on the following concepts:
 * - Query Subsumption {@link ReasonerAtomicQuery#subsumes(ReasonerAtomicQuery)}
 * Subsumption relation between a query C (child) and a provided query P (parent) holds if:
 *
 * C <= P,
 *
 * is true, meaning that P is more general than C (C specialises P)
 * and their respective answer sets meet:
 *
 * answers(C) subsetOf answers(P)
 *
 * i. e. the set of answers of C is a subset of the set of answers of P.
 *
 * - query semantic difference {@link SemanticDifference}
 * Semantic difference between query C and P defines a specialisation operation
 * required to transform query P into a query equivalent to C.
 *
 * Those concepts form the basis of the operation of the semantic cache:
 * if we are looking for answers to query C and the cache already contains query P which has all DB entries (db-complete),
 * we can propagate the answers, possibly specialising them (applying the semantic difference).
 *
 * @param <QE> cache entry query type
 * @param <SE> cache entry storage type
 */
public abstract class SemanticCache<
        QE,
        SE extends Set<ConceptMap>> extends QueryCacheBase<ReasonerAtomicQuery, Set<ConceptMap>, QE, SE> {

    final private HashMultimap<SchemaConcept, QE> families = HashMultimap.create();
    final private HashMultimap<QE, QE> parents = HashMultimap.create();

    private static final Logger LOG = LoggerFactory.getLogger(SemanticCache.class);

    UnifierType semanticUnifier(){ return UnifierType.RULE;}

    protected abstract void propagateAnswers(ReasonerAtomicQuery target, CacheEntry<ReasonerAtomicQuery, SE> parentEntry, CacheEntry<ReasonerAtomicQuery, SE> childEntry, boolean inferred);

    protected abstract Stream<ConceptMap> entryToAnswerStream(CacheEntry<ReasonerAtomicQuery, SE> entry);

    protected abstract Pair<Stream<ConceptMap>, MultiUnifier> entryToAnswerStreamWithUnifier(ReasonerAtomicQuery query, CacheEntry<ReasonerAtomicQuery, SE> entry);

    abstract CacheEntry<ReasonerAtomicQuery, SE> createEntry(ReasonerAtomicQuery query, Set<ConceptMap> answers);

    private CacheEntry<ReasonerAtomicQuery, SE> addEntry(CacheEntry<ReasonerAtomicQuery, SE> entry){
        CacheEntry<ReasonerAtomicQuery, SE> cacheEntry = putEntry(entry);
        ReasonerAtomicQuery query = entry.query();
        updateFamily(query);
        computeParents(query);
        propagateAnswersTo(query, query.isGround());
        return cacheEntry;
    }

    final private Set<ReasonerAtomicQuery> dbCompleteQueries = new HashSet<>();
    final private Set<QE> dbCompleteEntries = new HashSet<>();

    private boolean isDBComplete(ReasonerAtomicQuery query){
        return dbCompleteEntries.contains(queryToKey(query))
                || dbCompleteQueries.contains(query);
    }

    public void ackDBCompleteness(ReasonerAtomicQuery query){
        if (query.getAtom().getPredicates(IdPredicate.class).findFirst().isPresent()) {
            dbCompleteQueries.add(query);
        } else {
            dbCompleteEntries.add(queryToKey(query));
            LOG.trace("Cache db complete for: " + query);
        }
    }

    private void ackDBCompletenessFromParent(ReasonerAtomicQuery query, ReasonerAtomicQuery parent){
        if (dbCompleteQueries.contains(parent)) dbCompleteQueries.add(query);
        if (dbCompleteEntries.contains(queryToKey(parent))){
            dbCompleteEntries.add(queryToKey(query));
            LOG.trace("Cache db complete for: " + query);
        }
    }


    private Set<QE> getParents(ReasonerAtomicQuery child){
        Set<QE> parents = this.parents.get(queryToKey(child));
        if (!parents.isEmpty()) return parents;
        return computeParents(child);
    }

    private Set<QE> getFamily(ReasonerAtomicQuery query){
        SchemaConcept schemaConcept = query.getAtom().getSchemaConcept();
        return schemaConcept != null? families.get(schemaConcept) : null;
    }

    private void updateFamily(ReasonerAtomicQuery query){
        SchemaConcept schemaConcept = query.getAtom().getSchemaConcept();
        if (schemaConcept != null){
            Set<QE> familyEntry = families.get(schemaConcept);
            QE familyQuery = queryToKey(query);
            if (familyEntry != null){
                familyEntry.add(familyQuery);
            } else {
                families.put(schemaConcept, familyQuery);
            }
        }
    }

    private Set<QE> computeParents(ReasonerAtomicQuery child){
        Set<QE> family = getFamily(child);
        Set<QE> computedParents = new HashSet<>();
        if (family != null) {
            family.stream()
                    .map(this::keyToQuery)
                    .filter(parent -> !unifierType().equivalence().equivalent(child, parent))
                    .filter(child::subsumes)
                    .map(this::queryToKey)
                    .peek(computedParents::add)
                    .forEach(parent -> parents.put(queryToKey(child), parent));
        }
        return computedParents;
    }

    //NB: uses getEntry
    private void propagateAnswersTo(ReasonerAtomicQuery target, boolean inferred){
        CacheEntry<ReasonerAtomicQuery, SE> childMatch = getEntry(target);
        if (childMatch != null) {
            ReasonerAtomicQuery child = childMatch.query();
            parents.get(queryToKey(child))
                    .stream()
                    .filter(parent -> isDBComplete(keyToQuery(parent)))
                    .map(this::keyToQuery)
                    .map(this::getEntry)
                    .forEach(parentMatch -> {
                                propagateAnswers(target, parentMatch, childMatch, inferred);
                                ackDBCompletenessFromParent(target, parentMatch.query());
                            }
                    );
        }
    }

    @Override
    public CacheEntry<ReasonerAtomicQuery, SE> record(
            ReasonerAtomicQuery query,
            ConceptMap answer,
            @Nullable CacheEntry<ReasonerAtomicQuery, SE> entry,
            @Nullable MultiUnifier unifier) {
        /*
         * find SE entry
         * - if entry exists - easy
         * - if not, add entry and establish whether any parents present
         */
        CacheEntry<ReasonerAtomicQuery, SE> match = entry != null? entry : this.getEntry(query);
        if (match != null){

            ReasonerAtomicQuery equivalentQuery = match.query();
            SE answerSet = match.cachedElement();
            MultiUnifier multiUnifier = unifier == null? query.getMultiUnifier(equivalentQuery, unifierType()) : unifier;
            Set<Variable> cacheVars = equivalentQuery.getVarNames();
            //NB: this indexes answer according to all indices in the set
            multiUnifier
                    .apply(answer)
                    .peek(ans -> {
                        if (!ans.vars().equals(cacheVars)){
                            throw GraqlQueryException.invalidQueryCacheEntry(equivalentQuery, ans);
                        }
                    })
                    .forEach(answerSet::add);
            return match;
        }
        return addEntry(createEntry(query, Sets.newHashSet(answer)));
    }


    private Pair<Stream<ConceptMap>, MultiUnifier> getDBAnswerStreamWithUnifier(ReasonerAtomicQuery query){
        return new Pair<>(
                structuralCache().get(query),
                MultiUnifierImpl.trivial()
        );
    }
    @Override
    public Pair<Stream<ConceptMap>, MultiUnifier> getAnswerStreamWithUnifier(ReasonerAtomicQuery query) {
        CacheEntry<ReasonerAtomicQuery, SE> match = getEntry(query);

        if (match != null) {
            //extra check is a quasi-completeness check if there's no parent present we have no guarantees about completeness with respect to the db.
            Pair<Stream<ConceptMap>, MultiUnifier> cachePair = entryToAnswerStreamWithUnifier(query, match);
            return isDBComplete(query)?
                    cachePair :
                    new Pair<>(
                            Stream.concat(
                                    getDBAnswerStreamWithUnifier(query).getKey(),
                                    cachePair.getKey().filter(ans -> ans.explanation().isRuleExplanation())
                            ),
                            cachePair.getValue());
        }

        //if no match but db-complete parent exists, use parent to create entry
        Set<QE> parents = getParents(query);
        if (!parents.isEmpty() && parents.stream().anyMatch(p -> isDBComplete(keyToQuery(p)))){
            CacheEntry<ReasonerAtomicQuery, SE> newEntry = addEntry(createEntry(query, new HashSet<>()));
            return new Pair<>(entryToAnswerStream(newEntry), MultiUnifierImpl.trivial());
        }
        return getDBAnswerStreamWithUnifier(query);
    }


    @Override
    public CacheEntry<ReasonerAtomicQuery, SE> record(ReasonerAtomicQuery query, Set<ConceptMap> answers) {
        ConceptMap first = answers.stream().findFirst().orElse(null);
        if (first == null) return null;
        CacheEntry<ReasonerAtomicQuery, SE> record = record(query, first);
        Sets.difference(answers, Sets.newHashSet(first)).forEach(ans -> record(query, ans, record, null));
        return record;
    }

    @Override
    public CacheEntry<ReasonerAtomicQuery, SE> record(ReasonerAtomicQuery query, Stream<ConceptMap> answers) {
        return record(query, answers.collect(toSet()));
    }

    @Override
    public Stream<ConceptMap> getAnswerStream(ReasonerAtomicQuery query) {
        return getAnswerStreamWithUnifier(query).getKey();
    }

    @Override
    public Set<ConceptMap> getAnswers(ReasonerAtomicQuery query) {
        return getAnswerStream(query).collect(toSet());
    }

    @Override
    public Pair<Set<ConceptMap>, MultiUnifier> getAnswersWithUnifier(ReasonerAtomicQuery query) {
        Pair<Stream<ConceptMap>, MultiUnifier> answerStreamWithUnifier = getAnswerStreamWithUnifier(query);
        return new Pair<>(
                answerStreamWithUnifier.getKey().collect(toSet()),
                answerStreamWithUnifier.getValue()
        );
    }

    @Override
    public ConceptMap findAnswer(ReasonerAtomicQuery query, ConceptMap ans) {
        if(ans.isEmpty()) return ans;
        ConceptMap answer = getAnswerStreamWithUnifier(ReasonerQueries.atomic(query, ans)).getKey().findFirst().orElse(null);
        if (answer != null) return answer;

        //TODO should it create a cache entry?
        List<ConceptMap> answers = query.tx().execute(ReasonerQueries.create(query, ans).getQuery(), false);
        return answers.isEmpty()? new ConceptMap() : answers.iterator().next();
    }
}
