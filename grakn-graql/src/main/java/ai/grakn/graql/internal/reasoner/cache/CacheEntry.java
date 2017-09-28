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

import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;

/**
 *
 * <p>
 * Simple class for defining query entries.
 * </p>
 *
 * @param <Q> query type the entry corresponds to
 * @param <T> corresponding element to be cached
 *
 * @author Kasper Piskorski
 *
 */
class CacheEntry<Q extends ReasonerQueryImpl, T> {

    private final Q query;
    private final T cachedElement;

    CacheEntry(Q query, T element){
        this.query = query;
        this.cachedElement = element;
    }

    public Q query(){ return query;}
    public T cachedElement(){ return cachedElement;}
}
