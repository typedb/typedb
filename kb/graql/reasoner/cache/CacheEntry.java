/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.kb.graql.reasoner.cache;

import grakn.core.kb.graql.reasoner.query.ReasonerQuery;

/**
 * Simple class for defining query entries.
 *
 * @param <Q> query type the entry corresponds to
 * @param <T> corresponding element to be cached
 */
public class CacheEntry<Q extends ReasonerQuery, T> {

    private final Q query;
    private final T cachedElement;

    public CacheEntry(Q query, T element) {
        this.query = query;
        this.cachedElement = element;
    }

    public Q query() { return query;}

    public T cachedElement() { return cachedElement;}
}
