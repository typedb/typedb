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

package grakn.core.graph.graphdb.database.idassigner;

import grakn.core.graph.diskstorage.IDAuthority;

import java.time.Duration;

/**
 * The IDBlockSizer specifies the block size for
 * each partition guaranteeing that the same partition will always be assigned the same block size.
 */

public interface IDBlockSizer {

    /**
     * The size of the id block to be returned by calls {@link IDAuthority#getIDBlock(int, int, Duration)}
     * for the given id namespace.
     * In other words, for the returned array of the above mentioned call, it must hold that the difference between the second
     * and first value is equal to the block size returned by this method (for the same partition id).
     */
    long getBlockSize(int idNamespace);

    /**
     * Returns the upper bound for any id block returned by {@link IDAuthority#getIDBlock(int, int, Duration)}
     * for the given id namespace.
     * In other words, it must hold that the second value of the returned array is smaller than this value for the same partition id.
     */
    long getIdUpperBound(int idNamespace);

}
