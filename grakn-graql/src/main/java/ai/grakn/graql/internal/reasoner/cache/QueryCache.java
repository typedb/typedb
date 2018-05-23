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

import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.MultiUnifier;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.graql.internal.reasoner.MultiUnifierImpl;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.utils.Pair;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * <p>
 * Container class for storing performed query resolutions.
 * </p>
 *
 * @param <Q> the type of query that is being cached
 *
 * @author Kasper Piskorski
 *
 */
public class QueryCache<Q extends ReasonerQueryImpl> extends Cache<Q, QueryAnswers> {

    public QueryCache(){
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
    public Stream<Answer> record(Q query, Stream<Answer> answerStream) {
        //NB: output collection!
        QueryAnswers newAnswers = new QueryAnswers(answerStream.collect(Collectors.toSet()));
        return record(query, newAnswers).stream();
    }

    /**
     * record a specific answer to a given query
     * @param query to which an answer is to be recorded
     * @param answer specific answer to the query
     * @return recorded answer
     */
    public Answer recordAnswer(Q query, Answer answer){
        if(answer.isEmpty()) return answer;
        CacheEntry<Q, QueryAnswers> match =  this.getEntry(query);
        if (match != null) {
            Q equivalentQuery = match.query();
            QueryAnswers answers = match.cachedElement();
            MultiUnifier multiUnifier = query.getMultiUnifier(equivalentQuery);
            answer.unify(multiUnifier).forEach(answers::add);
        } else {
            this.putEntry(query, new QueryAnswers(answer));
        }
        return answer;
    }

    /**
     * record a specific answer to a given query with a known cache unifier
     * @param query to which an answer is to be recorded
     * @param answer answer specific answer to the query
     * @param unifier between the cached and input query
     * @return recorded answer
     */
    public Answer recordAnswerWithUnifier(Q query, Answer answer, MultiUnifier unifier){
        if(answer.isEmpty()) return answer;
        CacheEntry<Q, QueryAnswers> match =  this.getEntry(query);
        if (match != null) {
            QueryAnswers answers = match.cachedElement();
            answer.unify(unifier).forEach(answers::add);
        } else {
            this.putEntry(query, new QueryAnswers(answer));
        }
        return answer;
    }

    @Override
    public QueryAnswers getAnswers(Q query) {
        return getAnswersWithUnifier(query).getKey();
    }

    @Override
    public Stream<Answer> getAnswerStream(Q query) {
        return getAnswerStreamWithUnifier(query).getKey();
    }

    @Override
    public Pair<QueryAnswers, MultiUnifier> getAnswersWithUnifier(Q query) {
        Pair<Stream<Answer>, MultiUnifier> answerStreamWithUnifier = getAnswerStreamWithUnifier(query);
        return new Pair<>(
                new QueryAnswers(answerStreamWithUnifier.getKey().collect(Collectors.toSet())),
                answerStreamWithUnifier.getValue()
        );
    }

    @Override
    public Pair<Stream<Answer>, MultiUnifier> getAnswerStreamWithUnifier(Q query) {
        CacheEntry<Q, QueryAnswers> match =  this.getEntry(query);
        if (match != null) {
            Q equivalentQuery = match.query();
            QueryAnswers answers = match.cachedElement();
            MultiUnifier multiUnifier = equivalentQuery.getMultiUnifier(query);

            //NB: this is not lazy
            //lazy version would be answers.output().flatMap(ans -> ans.unify(multiUnifier))
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
    public Answer getAnswer(Q query, Answer ans){
        if(ans.isEmpty()) return ans;
        CacheEntry<Q, QueryAnswers> match =  this.getEntry(query);
        if (match != null) {
            Q equivalentQuery = match.query();
            MultiUnifier multiUnifier = equivalentQuery.getMultiUnifier(query);

            //NB: only used when checking for materialised answer duplicates
            Answer answer = match.cachedElement().stream()
                    .flatMap(a -> a.unify(multiUnifier))
                    .filter(a -> a.containsAll(ans))
                    .findFirst().orElse(null);
            if (answer != null) return answer;
        }

        //TODO should it create a cache entry?
        List<Answer> answers = ReasonerQueries.create(query, ans).getQuery().execute();
        return answers.isEmpty()? new QueryAnswer() : answers.iterator().next();
    }

    @Override
    public void remove(Cache<Q, QueryAnswers> c2, Set<Q> queries) {
        c2.getQueries().stream()
                .filter(queries::contains)
                .filter(this::contains)
                .forEach( q -> this.getEntry(q).cachedElement().removeAll(c2.getAnswers(q)));
    }

}
