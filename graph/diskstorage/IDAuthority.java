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
 */

package grakn.core.graph.diskstorage;


import grakn.core.graph.diskstorage.keycolumnvalue.KeyRange;

import java.time.Duration;
import java.util.List;

/**
 * Handles the unique allocation of ids. Returns blocks of ids that are uniquely allocated to the caller so that
 * they can be used to uniquely identify elements. *
 *
 */

public interface IDAuthority {

    /**
     * Returns a block of new ids in the form of IDBlock. It is guaranteed that
     * the block of ids for the particular partition id is uniquely assigned,
     * that is, the block of ids has not been previously and will not
     * subsequently be assigned again when invoking this method on the local or
     * any remote machine that is connected to the underlying storage backend.
     * <p>
     * In other words, this method has to ensure that ids are uniquely assigned
     * per partition.
     * <p>
     * It is furthermore guaranteed that any id of the returned IDBlock is smaller than the upper bound
     * for the given partition as read from the IDBlockSizer set on this IDAuthority and that the
     * number of ids returned is equal to the block size of the IDBlockSizer.
     *
     * @param partition
     *            Partition for which to request an id block
     * @param idNamespace namespace for ids within a partition
     * @param timeout
     *            When a call to this method is unable to return a id block
     *            before this timeout elapses, the implementation must give up
     *            and throw a {@code StorageException} ASAP
     * @return a range of ids for the {@code partition} parameter
     */
    IDBlock getIDBlock(int partition, int idNamespace, Duration timeout)
            throws BackendException;

    /**
     * Returns the lower and upper limits of the key range assigned to this local machine as an array with two entries.
     */
    List<KeyRange> getLocalIDPartition() throws BackendException;

    /**
     * Closes the IDAuthority and any underlying storage backend.
     */
    void close() throws BackendException;

    /**
     * Return the globally unique string used by this {@code IDAuthority}
     * instance to recognize its ID allocations and distinguish its allocations
     * from those belonging to other {@code IDAuthority} instances.
     *
     * This should normally be the value of
     * GraphDatabaseConfiguration#UNIQUE_INSTANCE_ID, though that's not
     * strictly technically necessary.
     *
     * @return unique ID string
     */
    String getUniqueID();

    /**
     * Whether #getIDBlock(int, int, Duration) may be safely interrupted.
     *
     * @return true if interruption is allowed, false if it is not
     */
    boolean supportsInterruption();

}
