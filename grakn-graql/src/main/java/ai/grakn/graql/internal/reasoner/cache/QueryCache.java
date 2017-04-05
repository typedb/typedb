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

import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.internal.reasoner.iterator.LazyIterator;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import javafx.util.Pair;

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
public class QueryCache<Q extends ReasonerQuery> extends Cache<Q, QueryAnswers> {

    public QueryCache(){ super();}
    public QueryCache(boolean explanation){ super(explanation);}

    @Override
    public QueryAnswers record(Q query, QueryAnswers answers) {
        Q equivalentQuery = contains(query)? cache.get(query).getKey() : null;
        if (equivalentQuery != null) {
            QueryAnswers unifiedAnswers = QueryAnswers.getUnifiedAnswers(equivalentQuery, query, answers);
            cache.get(query).getValue().addAll(unifiedAnswers);
        } else {
            cache.put(query, new Pair<>(query, answers));
        }
        return getAnswers(query);
    }

    @Override
    public Stream<Answer> record(Q query, Stream<Answer> answerStream) {
        QueryAnswers answers = new QueryAnswers(answerStream.collect(Collectors.toSet()));
        Pair<Q, QueryAnswers> match =  cache.get(query);
        if (match != null) {
            QueryAnswers unifiedAnswers = answers.unify(getRecordUnifier(query));
            match.getValue().addAll(unifiedAnswers);
            return match.getValue().stream();
        } else {
            cache.put(query, new Pair<>(query, answers));
            return answers.stream();
        }
    }

    public Answer recordAnswer(Q query, Answer answer){
        Pair<Q, QueryAnswers> match =  cache.get(query);
        if (match != null) {
            Answer unifiedAnswer = answer.unify(getRecordUnifier(query));
            match.getValue().add(unifiedAnswer);
        } else {
            cache.put(query, new Pair<>(query, new QueryAnswers(answer)));
        }
        return answer;
    }

    @Override
    public LazyIterator<Answer> recordRetrieveLazy(Q query, Stream<Answer> answers) {
        return new LazyIterator<>(record(query, answers));
    }

    @Override
    public QueryAnswers getAnswers(Q query) {
        Q equivalentQuery = contains(query)? cache.get(query).getKey() : null;
        if (equivalentQuery != null) {
            return QueryAnswers.getUnifiedAnswers(query, equivalentQuery, cache.get(equivalentQuery).getValue());
        }
        else return new QueryAnswers();
    }

    @Override
    public Stream<Answer> getAnswerStream(Q query) {
        return getAnswers(query).stream();
    }


    @Override
    public LazyIterator<Answer> getAnswerIterator(Q query) {
        return new LazyIterator<>(getAnswers(query).stream());
    }

    @Override
    public void remove(Cache<Q, QueryAnswers> c2, Set<Q> queries) {
        c2.cache.keySet().stream()
                .filter(queries::contains)
                .filter(this::contains)
                .forEach( q -> cache.get(q).getValue().removeAll(c2.getAnswers(q)));
    }

    @Override
    public long answerSize(Set<Q> queries) {
        return cache.values().stream()
                .filter(p -> queries.contains(p.getKey()))
                .map(v -> v.getValue().size()).mapToInt(Integer::intValue).sum();
    }
}
