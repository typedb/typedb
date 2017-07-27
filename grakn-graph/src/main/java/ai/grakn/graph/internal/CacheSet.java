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

package ai.grakn.graph.internal;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

/**
 * <p>
 *     An internal cached {@link java.util.Set}
 * </p>
 *
 * <p>
 *     An internal cached object which hits the database only when it needs to.
 *     This is used to expand on the functionality provided by {@link Cache}.
 *     This particular class ensures that no cache leaks can occur when dealing with {@link java.util.Set}
 *     which can be accidentally leak due to {@link Cache#valueGlobal} being shared between transactions.
 *
 *     This class facilitates the deep cloning of {@link java.util.Set} to prevent such leaks.
 * </p>
 *
 * @author fppt
 *
 */
class CacheSet<V> extends Cache<Set<V>> {
    CacheSet(Supplier<Set<V>> databaseReader){
        super(databaseReader);
    }

    /**
     * Copies the cached {@link Set} into a new {@link Set} to prevent leaks.
     *
     * @return The newly copied cached {@link Set}
     */
    @Override
    Set<V> copyGlobal(){
        return new HashSet<>(super.copyGlobal());
    }
}
