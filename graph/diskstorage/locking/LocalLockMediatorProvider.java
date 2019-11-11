/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graph.diskstorage.locking;

import grakn.core.graph.diskstorage.util.time.TimestampProvider;

/**
 * Service provider interface for {@link LocalLockMediators}.
 */
public interface LocalLockMediatorProvider {

    /**
     * Returns a the single {@link LocalLockMediator} responsible for the
     * specified {@code namespace}.
     * <p>
     * For any given {@code namespace}, the same object must be returned every
     * time {@code get(n)} is called, no matter what thread calls it or how many
     * times.
     * <p>
     * For any two unequal namespace strings {@code n} and {@code m},
     * {@code get(n)} must not equal {@code get(m)}. in other words, each
     * namespace must have a distinct mediator.
     *
     * @param namespace the arbitrary identifier for a local lock mediator
     * @return the local lock mediator for {@code namespace}
     * @see LocalLockMediator
     */
    <T> LocalLockMediator<T> get(String namespace, TimestampProvider times);

}
