/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.reasoner.cache;

import ai.grakn.concept.ConceptId;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.graql.internal.query.match.MatchBase;
import ai.grakn.graql.internal.reasoner.explanation.LookupExplanation;
import ai.grakn.graql.internal.reasoner.iterator.LazyIterator;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.graql.internal.reasoner.UnifierImpl;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.utils.Pair;

import java.util.List;
import java.util.Map;
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
        Pair<Q, QueryAnswers> match =  this.get(query);
        if (match != null) {
            Q equivalentQuery = match.getKey();
            QueryAnswers unifiedAnswers = QueryAnswers.getUnifiedAnswers(equivalentQuery, query, answers);
            this.get(query).getValue().addAll(unifiedAnswers);
            return getAnswers(query);
        }
        this.put(query, answers);
        return answers;
    }

    @Override
    public Stream<Answer> record(Q query, Stream<Answer> answerStream) {
        //NB: stream collection!
        QueryAnswers newAnswers = new QueryAnswers(answerStream.collect(Collectors.toSet()));
        Pair<Q, QueryAnswers> match =  this.get(query);
        if (match != null) {
            Q equivalentQuery = match.getKey();
            QueryAnswers answers = match.getValue();
            QueryAnswers unifiedAnswers = newAnswers.unify(query.getUnifier(equivalentQuery));
            answers.addAll(unifiedAnswers);
            return answers.stream();
        }
        this.put(query, newAnswers);
        return newAnswers.stream();
    }

    @Override
    public LazyIterator<Answer> recordRetrieveLazy(Q query, Stream<Answer> answers) {
        return new LazyIterator<>(record(query, answers));
    }

    /**
     * record a specific answer to a given query
     * @param query to which an answer is to be recorded
     * @param answer specific answer to the query
     * @return recorded answer
     */
    public Answer recordAnswer(Q query, Answer answer){
        if(answer.isEmpty()) return answer;
        Pair<Q, QueryAnswers> match =  this.get(query);
        if (match != null) {
            Q equivalentQuery = match.getKey();
            QueryAnswers answers = match.getValue();
            Answer unifiedAnswer = answer.unify(query.getUnifier(equivalentQuery));
            answers.add(unifiedAnswer);
        } else {
            this.put(query, new QueryAnswers(answer));
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
    public Answer recordAnswerWithUnifier(Q query, Answer answer, Unifier unifier){
        if(answer.isEmpty()) return answer;
        Pair<Q, QueryAnswers> match =  this.get(query);
        if (match != null) {
            QueryAnswers answers = match.getValue();
            Answer unifiedAnswer = answer.unify(unifier);
            answers.add(unifiedAnswer);
        } else {
            this.put(query, new QueryAnswers(answer));
        }
        return answer;
    }

    @Override
    public QueryAnswers getAnswers(Q query) {
        return getAnswersWithUnifier(query).getKey();
    }

    @Override
    public Pair<QueryAnswers, Unifier> getAnswersWithUnifier(Q query) {
        Pair<Q, QueryAnswers> match =  this.get(query);
        if (match != null) {
            Q equivalentQuery = match.getKey();
            QueryAnswers answers = match.getValue();
            Unifier unifier = equivalentQuery.getUnifier(query);
            return new Pair<>(answers.unify(unifier), unifier);
        }
        return new Pair<>(new QueryAnswers(), new UnifierImpl());
    }

    @Override
    public Stream<Answer> getAnswerStream(Q query) {
        return getAnswers(query).stream();
    }

    @Override
    public Pair<Stream<Answer>, Unifier> getAnswerStreamWithUnifier(Q query) {
        Pair<Q, QueryAnswers> match =  this.get(query);
        if (match != null) {
            Q equivalentQuery = match.getKey();
            QueryAnswers answers = match.getValue();
            Unifier unifier = equivalentQuery.getUnifier(query);
            return new Pair<>(answers.unify(unifier).stream(), unifier);
        }

        Pair<StructuralCacheEntry<Q>, Pair<Unifier, Map<ConceptId, ConceptId>>> cacheEntry = structuralCache().get(query);
        Unifier unifier = cacheEntry.getValue().getKey();
        Map<ConceptId, ConceptId> conceptMap = cacheEntry.getValue().getValue();
        ReasonerQueryImpl cacheQuery = cacheEntry.getKey().query().transformIds(conceptMap);

        Stream<Answer> answerStream = MatchBase.streamWithTraversal(cacheQuery.getPattern(), cacheQuery.tx(), cacheEntry.getKey().traversal())
                .map(ans -> ans.unify(unifier))
                .map(a -> a.explain(new LookupExplanation(query)));
        return new Pair<>(answerStream, new UnifierImpl());
    }

    @Override
    public LazyIterator<Answer> getAnswerIterator(Q query) {
        return new LazyIterator<>(getAnswers(query).stream());
    }

    /**
     * find specific answer to a query in the cache
     * @param query input query
     * @param ans sought specific answer to the query
     * @return found answer if any, otherwise empty answer
     */
    public Answer getAnswer(Q query, Answer ans){
        if(ans.isEmpty()) return ans;
        Pair<Q, QueryAnswers> match =  this.get(query);
        if (match != null) {
            Q equivalentQuery = match.getKey();
            Unifier unifier = equivalentQuery.getUnifier(query);
            QueryAnswers answers =  match.getValue().unify(unifier);
            Answer answer = answers.stream()
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
                .forEach( q -> this.get(q).getValue().removeAll(c2.getAnswers(q)));
    }

    @Override
    public long answerSize(Set<Q> queries) {
        return entries().stream()
                .filter(p -> queries.contains(p.getKey()))
                .map(v -> v.getValue().size()).mapToInt(Integer::intValue).sum();
    }
}
