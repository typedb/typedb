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

package ai.grakn.graph.internal.cache;

import ai.grakn.concept.Concept;

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
public interface ContainsTxCache {

    /**
     * Clears the internal {@link Cache}
     */
    void txCacheClear();

    /**
     * Helper method to cast {@link Concept} into {@link ContainsTxCache}
     */
    static ContainsTxCache from(Concept concept){
        return (ContainsTxCache) concept;
    }
}
