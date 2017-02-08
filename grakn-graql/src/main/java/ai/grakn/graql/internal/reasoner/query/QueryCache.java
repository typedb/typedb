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

package ai.grakn.graql.internal.reasoner.query;

import java.util.HashMap;
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
public class QueryCache extends HashMap<ReasonerAtomicQuery, Pair<ReasonerAtomicQuery, QueryAnswers>> {

    public QueryCache(){ super();}
    public boolean contains(ReasonerAtomicQuery query){ return this.containsKey(query);}

    /**
     * updates the cache by the specified query
     * @param atomicQuery query to be added/updated
     * @param answers answers to the query
     */
    public void record(ReasonerAtomicQuery atomicQuery, QueryAnswers answers){
        ReasonerAtomicQuery equivalentQuery = contains(atomicQuery)? get(atomicQuery).getKey() : null;
        if (equivalentQuery != null) {
            QueryAnswers unifiedAnswers = QueryAnswers.getUnifiedAnswers(equivalentQuery, atomicQuery, answers);
            get(atomicQuery).getValue().addAll(unifiedAnswers);
        } else {
            put(atomicQuery, new Pair<>(atomicQuery, answers));
        }
    }

    public QueryAnswers getAnswers(ReasonerAtomicQuery q){
        ReasonerAtomicQuery equivalentQuery = contains(q)? get(q).getKey() : null;
        if (equivalentQuery != null) {
            return QueryAnswers.getUnifiedAnswers(q, equivalentQuery, get(q).getValue());
        }
        else return new QueryAnswers();
    }

    public int answerSize(){
        return keySet().stream().map(q -> get(q).getValue().size()).mapToInt(Integer::intValue).sum();
    }
}
