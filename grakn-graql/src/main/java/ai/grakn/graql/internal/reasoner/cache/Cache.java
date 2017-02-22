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
import ai.grakn.graql.internal.reasoner.iterator.LazyIterator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import javafx.util.Pair;

/**
 *
 * <p>
 * Generic container class for storing performed query resolutions.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public abstract class Cache<Q extends ReasonerQuery, T extends Iterable<Map<VarName, Concept>>>{

    protected final Map<Q, Pair<Q, T>> cache = new HashMap<>();

    public Cache(){ super();}
    public boolean contains(Q query){ return cache.containsKey(query);}

    public abstract T record(Q query, T answers);
    public abstract Stream<Map<VarName, Concept>> record(Q query, Stream<Map<VarName, Concept>> answers);
    public abstract LazyIterator<Map<VarName, Concept>> recordRetrieveLazy(Q query, Stream<Map<VarName, Concept>> answers);

    public abstract T getAnswers(Q query);
    public abstract Stream<Map<VarName, Concept>> getAnswerStream(Q query);
    public abstract LazyIterator<Map<VarName, Concept>> getAnswerIterator(Q query);

    Map<VarName, VarName> getRecordUnifiers(Q toRecord){
        Q equivalentQuery = contains(toRecord)? cache.get(toRecord).getKey() : null;
        if (equivalentQuery != null) return toRecord.getUnifiers(equivalentQuery);
        else return new HashMap<>();
    }

    Map<VarName, VarName> getRetrieveUnifiers(Q toRetrieve){
        Q equivalentQuery = contains(toRetrieve)? cache.get( toRetrieve).getKey() : null;
        if (equivalentQuery != null) return  equivalentQuery.getUnifiers(toRetrieve);
        else return new HashMap<>();
    }

    public abstract long answerSize(Set<Q> queries);
}
