/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package grakn.core.server.kb.cache;

import grakn.core.graql.concept.Concept;

import java.util.Collection;

/**
 * Indicates a Cache is contained within the class
 * Wraps up behaviour which needs to be handled whenever a Cache is used in a class
 */
public interface CacheOwner {

    /**
     * Helper method to cast Concept into CacheOwner
     */
    static CacheOwner from(Concept concept) {
        return (CacheOwner) concept;
    }

    /**
     * @return all the caches beloning to the CacheOwner
     */
    Collection<Cache> caches();

    /**
     * Clears the internal Cache
     */
    default void txCacheClear() {
        caches().forEach(Cache::clear);
    }

    /**
     * Registers a Cache so that later it can be cleaned up
     */
    default void registerCache(Cache cache) {
        caches().add(cache);
    }

    /**
     * Flushes the internal transaction caches so they can refresh with persisted graph
     */
    default void txCacheFlush() {
        caches().forEach(Cache::flush);
    }
}
