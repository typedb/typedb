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

package grakn.core.kb.internal.cache;

import grakn.core.concept.Concept;

import java.util.Collection;

/**
 * <p>
 *     Indicates a {@link Cache} is contained within the class
 * </p>
 *
 * <p>
 *     Wraps up behaviour which needs to be handled whenever a {@link Cache} is used in a class
 * </p>
 *
 *
 */
public interface CacheOwner {

    /**
     *
     * @return all the caches beloning to the {@link CacheOwner}
     */
    Collection<Cache> caches();

    /**
     * Clears the internal {@link Cache}
     */
    default void txCacheClear() {
        caches().forEach(Cache::clear);
    }

    /**
     * Registers a {@link Cache} so that later it can be cleaned up
     */
    default void registerCache(Cache cache) {
        caches().add(cache);
    }

    /**
     * Flushes the internal transaction caches so they can refresh with persisted graph
     */
    default void txCacheFlush(){
        caches().forEach(Cache::flush);
    }

    /**
     * Helper method to cast {@link Concept} into {@link CacheOwner}
     */
    static CacheOwner from(Concept concept){
        return (CacheOwner) concept;
    }
}
