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

import com.google.common.collect.Sets;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.reasoner.query.ReasonerQueries;
import grakn.core.graql.internal.reasoner.query.ReasonerQueryImpl;
import grakn.core.graql.internal.reasoner.unifier.MultiUnifier;
import grakn.core.graql.internal.reasoner.unifier.MultiUnifierImpl;
import grakn.core.graql.internal.reasoner.utils.Pair;
import graql.lang.statement.Variable;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

/**
 * Container class for storing performed query resolutions.
 * Cache operations are handled in the following way:
 * - GET(Query) - retrieve an entry corresponding to a provided query, if entry doesn't exist return db lookup result of the query.
 * - RECORD(Query) - if the query entry exists, update the entry, otherwise create a new entry. In each case return an up-to-date entry.
 *
 * @param <Q> the type of query that is being cached
 */
public class SimpleQueryCache<Q extends ReasonerQueryImpl> extends SimpleQueryCacheBase<Q, Set<ConceptMap>> {

    public SimpleQueryCache() {
        super();
    }

    @Override
    public CacheEntry<Q, Set<ConceptMap>> record(Q query, Set<ConceptMap> answers) {
        CacheEntry<Q, Set<ConceptMap>> match = this.getEntry(query);
        if (match != null) {
            Q equivalentQuery = match.query();
            MultiUnifier multiUnifier = query.getMultiUnifier(equivalentQuery, unifierType());
            Set<ConceptMap> unifiedAnswers = answers.stream().flatMap(ans -> ans.unify(multiUnifier)).collect(toSet());
            this.getEntry(query).cachedElement().addAll(unifiedAnswers);
            return match;
        }
        return putEntry(query, answers);
    }

    @Override
    public CacheEntry<Q, Set<ConceptMap>> record(Q query, Stream<ConceptMap> answerStream) {
        //NB: stream collection!
        Set<ConceptMap> newAnswers = answerStream.collect(toSet());
        return record(query, newAnswers);
    }

    /**
     * record a specific answer to a given query with a known cache unifier
     *
     * @param query   to which an answer is to be recorded
     * @param answer  answer specific answer to the query
     * @param unifier between the cached and input query
     * @return recorded answer
     */
    public CacheEntry<Q, Set<ConceptMap>> record(Q query, ConceptMap answer, @Nullable CacheEntry<Q, Set<ConceptMap>> entry, @Nullable MultiUnifier unifier) {
        CacheEntry<Q, Set<ConceptMap>> match = entry != null ? entry : this.getEntry(query);
        if (match != null) {
            Q equivalentQuery = match.query();
            Set<ConceptMap> answers = match.cachedElement();
            MultiUnifier multiUnifier = unifier == null ? query.getMultiUnifier(equivalentQuery, unifierType()) : unifier;

            Set<Variable> cacheVars = answers.isEmpty() ? new HashSet<>() : answers.iterator().next().vars();
            multiUnifier.stream()
                    .map(answer::unify)
                    .peek(ans -> {
                        if (!ans.vars().containsAll(cacheVars)) {
                            throw GraqlQueryException.invalidQueryCacheEntry(equivalentQuery, ans);
                        }
                    })
                    .forEach(answers::add);
            return match;
        } else {
            if (!answer.vars().containsAll(query.getVarNames())) {
                throw GraqlQueryException.invalidQueryCacheEntry(query, answer);
            }
            return putEntry(query, Sets.newHashSet(answer));
        }
    }

    @Override
    public Set<ConceptMap> getAnswers(Q query) {
        return getAnswersWithUnifier(query).getKey();
    }

    @Override
    public Stream<ConceptMap> getAnswerStream(Q query) {
        return getAnswerStreamWithUnifier(query).getKey();
    }

    @Override
    public Pair<Set<ConceptMap>, MultiUnifier> getAnswersWithUnifier(Q query) {
        Pair<Stream<ConceptMap>, MultiUnifier> answerStreamWithUnifier = getAnswerStreamWithUnifier(query);
        return new Pair<>(
                answerStreamWithUnifier.getKey().collect(toSet()),
                answerStreamWithUnifier.getValue()
        );
    }

    @Override
    public Pair<Stream<ConceptMap>, MultiUnifier> getAnswerStreamWithUnifier(Q query) {
        CacheEntry<Q, Set<ConceptMap>> match = this.getEntry(query);
        if (match != null) {
            Q equivalentQuery = match.query();
            Set<ConceptMap> answers = match.cachedElement();
            MultiUnifier multiUnifier = equivalentQuery.getMultiUnifier(query, unifierType());

            //NB: this is not lazy
            //lazy version would be answers.stream().flatMap(ans -> ans.unify(multiUnifier))
            //NB: Concurrent modification exception if lazy
            return new Pair<>(answers.stream().flatMap(ans -> ans.unify(multiUnifier)).collect(toSet()).stream(), multiUnifier);
        }
        return new Pair<>(
                structuralCache().get(query),
                new MultiUnifierImpl()
        );
    }

    @Override
    public ConceptMap findAnswer(Q query, ConceptMap ans) {
        if (ans.isEmpty()) return ans;
        CacheEntry<Q, Set<ConceptMap>> match = this.getEntry(query);
        if (match != null) {
            Q equivalentQuery = match.query();
            MultiUnifier multiUnifier = equivalentQuery.getMultiUnifier(query, unifierType());

            //NB: only used when checking for materialised answer duplicates
            ConceptMap answer = match.cachedElement().stream()
                    .flatMap(a -> a.unify(multiUnifier))
                    .filter(a -> a.containsAll(ans))
                    .findFirst().orElse(null);
            if (answer != null) return answer;
        }

        //TODO should it create a cache entry?
        List<ConceptMap> answers = query.tx().execute(ReasonerQueries.create(query, ans).getQuery(), false);
        return answers.isEmpty() ? new ConceptMap() : answers.iterator().next();
    }
}
