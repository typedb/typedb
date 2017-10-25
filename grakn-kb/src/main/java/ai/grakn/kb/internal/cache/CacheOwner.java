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

package ai.grakn.kb.internal.cache;

import ai.grakn.concept.Concept;

import java.util.HashSet;
import java.util.Set;

/**
 * <p>
 *     Indicates a {@link Cache} is contained within the class
 * </p>
 *
 * <p>
 *     Wraps up behaviour which needs to be handled whenever a {@link Cache} is used in a class
 * </p>
 *
 * @author fppt
 *
 */
public abstract class CacheOwner {
    //All the aches belonging to this object
    private final Set<Cache> registeredCaches = new HashSet<>();

    /**
     * Clears the internal {@link Cache}
     */
    protected final void txCacheClear() {
        registeredCaches.forEach(Cache::clear);
    }

    /**
     * Registers a {@link Cache} so that later it can be cleaned up
     */
    final void registerCache(Cache cache) {
        registeredCaches.add(cache);
    }

    /**
     * Flushes the internal transaction caches so they can refresh with persisted graph
     */
    protected final void txCacheFlush(){
        registeredCaches.forEach(Cache::flush);
    }

    /**
     * Helper method to cast {@link Concept} into {@link CacheOwner}
     */
    static CacheOwner from(Concept concept){
        return (CacheOwner) concept;
    }
}
