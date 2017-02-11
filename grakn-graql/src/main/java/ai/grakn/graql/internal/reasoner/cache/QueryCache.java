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
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javafx.util.Pair;

/**
 *
 * <p>
 * Container class for storing performed query resolutions.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class QueryCache<T extends ReasonerQuery>{

    private final Map<T, Pair<T, QueryAnswers>> cache = new HashMap<>();

    public QueryCache(){ super();}
    public boolean contains(T query){ return cache.containsKey(query);}

    /**
     * updates the cache by the specified query
     * @param atomicQuery query to be added/updated
     * @param answers answers to the query
     */
    public void record(T atomicQuery, QueryAnswers answers){
        T equivalentQuery = contains(atomicQuery)? cache.get(atomicQuery).getKey() : null;
        if (equivalentQuery != null) {
            QueryAnswers unifiedAnswers = QueryAnswers.getUnifiedAnswers(equivalentQuery, atomicQuery, answers);
            cache.get(atomicQuery).getValue().addAll(unifiedAnswers);
        } else {
            cache.put(atomicQuery, new Pair<>(atomicQuery, answers));
        }
    }

    public void record(T atomicQuery, Map<VarName, Concept> answer){
        record(atomicQuery, answer, getRecordUnifiers(atomicQuery));
    }

    public void record(T atomicQuery, Map<VarName, Concept> answer, Map<VarName, VarName> unifiers){
        T equivalentQuery = contains(atomicQuery)? cache.get(atomicQuery).getKey() : null;
        if (equivalentQuery != null) {
            cache.get(equivalentQuery).getValue().add(QueryAnswers.unify(answer, unifiers));
        } else {
            cache.put(atomicQuery, new Pair<>(atomicQuery, new QueryAnswers(Sets.newHashSet(Collections.singletonList(QueryAnswers.unify(answer, unifiers))))));
        }
    }

    public QueryAnswers getAnswers(T query){
        T equivalentQuery = contains(query)? cache.get(query).getKey() : null;
        if (equivalentQuery != null) {
            return QueryAnswers.getUnifiedAnswers(query, equivalentQuery, cache.get(equivalentQuery).getValue());
        }
        else return new QueryAnswers();
    }

    private Map<VarName, VarName> getRecordUnifiers(T toRecord){
        T equivalentQuery = contains(toRecord)? cache.get(toRecord).getKey() : null;
        if (equivalentQuery != null) return toRecord.getUnifiers(equivalentQuery);
        else return new HashMap<>();
    }

    public int answerSize(Set<T> queries){
        return cache.values().stream()
                .filter(p -> queries.contains(p.getKey()))
                .map(v -> v.getValue().size()).mapToInt(Integer::intValue).sum();
    }
}
