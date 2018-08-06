/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.reasoner.cache;

import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.MultiUnifier;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.internal.query.answer.ConceptMapImpl;
import ai.grakn.graql.internal.reasoner.MultiUnifierImpl;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.utils.Pair;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 *
 * <p>
 * Container class for storing performed query resolutions.
 *
 * Cache operations are handled in the following way:
 * - GET(Query) - retrieve an entry corresponding to a provided query, if entry doesn't exist return db lookup result of the query.
 * - RECORD(Query) - if the query entry exists, update the entry, otherwise create a new entry. In each case return an up-to-date entry.
 *
 * </p>
 *
 * @param <Q> the type of query that is being cached
 *
 * @author Kasper Piskorski
 *
 */
public class SimpleQueryCache<Q extends ReasonerQueryImpl> extends QueryCacheBase<Q, QueryAnswers> {

    public SimpleQueryCache(){
        super();
    }

    @Override
    public QueryAnswers record(Q query, QueryAnswers answers) {
        CacheEntry<Q, QueryAnswers> match =  this.getEntry(query);
        if (match != null) {
            Q equivalentQuery = match.query();
            QueryAnswers unifiedAnswers = answers.unify(query.getMultiUnifier(equivalentQuery));
            this.getEntry(query).cachedElement().addAll(unifiedAnswers);
            return getAnswers(query);
        }
        this.putEntry(query, answers);
        return answers;
    }

    @Override
    public Stream<ConceptMap> record(Q query, Stream<ConceptMap> answerStream) {
        //NB: stream collection!
        QueryAnswers newAnswers = new QueryAnswers(answerStream.collect(Collectors.toSet()));
        return record(query, newAnswers).stream();
    }

    /**
     * record a specific answer to a given query
     * @param query to which an answer is to be recorded
     * @param answer specific answer to the query
     * @return recorded answer
     */
    @Override
    public ConceptMap record(Q query, ConceptMap answer){
        return record(query, answer, null);
    }

    /**
     * record a specific answer to a given query with a known cache unifier
     * @param query to which an answer is to be recorded
     * @param answer answer specific answer to the query
     * @param unifier between the cached and input query
     * @return recorded answer
     */
    public ConceptMap record(Q query, ConceptMap answer, @Nullable MultiUnifier unifier){
        if(answer.isEmpty()) return answer;
        CacheEntry<Q, QueryAnswers> match =  this.getEntry(query);
        if (match != null) {
            Q equivalentQuery = match.query();
            QueryAnswers answers = match.cachedElement();
            MultiUnifier multiUnifier = unifier == null? query.getMultiUnifier(equivalentQuery) : unifier;

            Set<Var> cacheVars = answers.isEmpty()? new HashSet<>() : answers.iterator().next().vars();
            multiUnifier.stream()
                    .map(answer::unify)
                    .peek(ans -> {
                        if (!ans.vars().containsAll(cacheVars)){
                            throw GraqlQueryException.invalidQueryCacheEntry(equivalentQuery);
                        }
                    })
                    .forEach(answers::add);
        } else {
            if (!answer.vars().containsAll(query.getVarNames())){
                throw GraqlQueryException.invalidQueryCacheEntry(query);
            }
            this.putEntry(query, new QueryAnswers(answer));
        }
        return answer;
    }

    @Override
    public QueryAnswers getAnswers(Q query) {
        return getAnswersWithUnifier(query).getKey();
    }

    @Override
    public Stream<ConceptMap> getAnswerStream(Q query) {
        return getAnswerStreamWithUnifier(query).getKey();
    }

    @Override
    public Pair<QueryAnswers, MultiUnifier> getAnswersWithUnifier(Q query) {
        Pair<Stream<ConceptMap>, MultiUnifier> answerStreamWithUnifier = getAnswerStreamWithUnifier(query);
        return new Pair<>(
                new QueryAnswers(answerStreamWithUnifier.getKey().collect(Collectors.toSet())),
                answerStreamWithUnifier.getValue()
        );
    }

    @Override
    public Pair<Stream<ConceptMap>, MultiUnifier> getAnswerStreamWithUnifier(Q query) {
        CacheEntry<Q, QueryAnswers> match =  this.getEntry(query);
        if (match != null) {
            Q equivalentQuery = match.query();
            QueryAnswers answers = match.cachedElement();
            MultiUnifier multiUnifier = equivalentQuery.getMultiUnifier(query);

            //NB: this is not lazy
            //lazy version would be answers.stream().flatMap(ans -> ans.unify(multiUnifier))
            return new Pair<>(answers.unify(multiUnifier).stream(), multiUnifier);
        }
        return new Pair<>(
                structuralCache().get(query),
                new MultiUnifierImpl()
        );
    }

    /**
     * find specific answer to a query in the cache
     * @param query input query
     * @param ans sought specific answer to the query
     * @return found answer if any, otherwise empty answer
     */
    public ConceptMap getAnswer(Q query, ConceptMap ans){
        if(ans.isEmpty()) return ans;
        CacheEntry<Q, QueryAnswers> match =  this.getEntry(query);
        if (match != null) {
            Q equivalentQuery = match.query();
            MultiUnifier multiUnifier = equivalentQuery.getMultiUnifier(query);

            //NB: only used when checking for materialised answer duplicates
            ConceptMap answer = match.cachedElement().stream()
                    .flatMap(a -> a.unify(multiUnifier))
                    .filter(a -> a.containsAll(ans))
                    .findFirst().orElse(null);
            if (answer != null) return answer;
        }

        //TODO should it create a cache entry?
        List<ConceptMap> answers = ReasonerQueries.create(query, ans).getQuery().execute();
        return answers.isEmpty()? new ConceptMapImpl() : answers.iterator().next();
    }
}
