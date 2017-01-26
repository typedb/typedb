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

/**
 *
 * <p>
 * Container class for storing performed query resolutions.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class QueryCache extends HashMap<ReasonerAtomicQuery, ReasonerAtomicQuery> {

    public QueryCache(){ super();}
    public boolean contains(ReasonerAtomicQuery query){ return this.containsKey(query);}

    /**
     * updates the cache by the specified query
     * @param atomicQuery query to be added/updated
     */
    public void record(ReasonerAtomicQuery atomicQuery){
        ReasonerAtomicQuery equivalentQuery = get(atomicQuery);
        if (equivalentQuery != null) {
            QueryAnswers unifiedAnswers = QueryAnswers.getUnifiedAnswers(equivalentQuery, atomicQuery);
            get(atomicQuery).getAnswers().addAll(unifiedAnswers);
        } else {
            put(atomicQuery, atomicQuery);
        }
    }
}
