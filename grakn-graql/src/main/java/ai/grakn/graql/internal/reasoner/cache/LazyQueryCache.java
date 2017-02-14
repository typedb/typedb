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

import ai.grakn.concept.Concept;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.internal.reasoner.iterator.LazyAnswerIterator;
import ai.grakn.graql.internal.reasoner.query.QueryAnswerStream;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import javafx.util.Pair;

/**
 *
 * <p>
 * Lazy container class for storing performed query resolutions.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class LazyQueryCache<T extends ReasonerQuery>{

    public final Map<T, Pair<T, LazyAnswerIterator>> cache = new HashMap<>();

    public LazyQueryCache(){ super();}
    public boolean contains(T query){ return cache.containsKey(query);}

    /**
     * updates the cache by the specified query
     * @param query query to be added/updated
     * @param answers answers to the query
     */
    public LazyAnswerIterator record(T query, Stream<Map<VarName, Concept>> answers){
        Pair<T, LazyAnswerIterator> match =  cache.get(query);
        if (match!= null) {
            Stream<Map<VarName, Concept>> unifiedStream = QueryAnswerStream.unify(answers, getRecordUnifiers(query));
            cache.put(match.getKey(), new Pair<>(match.getKey(), match.getValue().merge(unifiedStream)));
        } else {
            cache.put(query, new Pair<>(query, new LazyAnswerIterator(answers)));
        }
        return getAnswerIterator(query);
    }

    /**
     * retrieve cached answers for provided query
     * @param query for which to retrieve answers
     * @return unified cached answers
     */
    public Stream<Map<VarName, Concept>> getAnswers(T query){
        Pair<T, LazyAnswerIterator> match =  cache.get(query);
        if (match != null) {
            Map<VarName, VarName> unifiers = getRetrieveUnifiers(query);
            return match.getValue().stream().map(a -> QueryAnswers.unify(a, unifiers));
        }
        else return Stream.empty();
    }

    public LazyAnswerIterator getAnswerIterator(T query){
        Pair<T, LazyAnswerIterator> match =  cache.get(query);
        if (match != null) {
            return match.getValue().unify(getRetrieveUnifiers(query));
        }
        else return new LazyAnswerIterator(Stream.empty());
    }

    private Map<VarName, VarName> getRecordUnifiers(T toRecord){
        T equivalentQuery = contains(toRecord)? cache.get(toRecord).getKey() : null;
        if (equivalentQuery != null) return toRecord.getUnifiers(equivalentQuery);
        else return new HashMap<>();
    }

    private Map<VarName, VarName> getRetrieveUnifiers(T toRetrieve){
        T equivalentQuery = contains(toRetrieve)? cache.get( toRetrieve).getKey() : null;
        if (equivalentQuery != null) return  equivalentQuery.getUnifiers(toRetrieve);
        else return new HashMap<>();
    }

    public void reload(){
        Map<T, Pair<T, LazyAnswerIterator>> newCache = new HashMap<>();
        cache.entrySet().forEach(entry ->
                newCache.put(entry.getKey(), new Pair<>(
                        entry.getKey(),
                        new LazyAnswerIterator(entry.getValue().getValue().accumulator.stream()))));
        cache.clear();
        cache.putAll(newCache);
    }

    public long answerSize(Set<T> queries){
        return cache.values().stream()
                .filter(p -> queries.contains(p.getKey()))
                .map(v -> v.getValue().size()).mapToLong(Long::longValue).sum();
    }
}
