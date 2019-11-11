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

import grakn.core.graph.diskstorage.keycolumnvalue.StoreTransaction;
import grakn.core.graph.diskstorage.locking.PermanentLockingException;
import grakn.core.graph.diskstorage.locking.TemporaryLockingException;
import grakn.core.graph.diskstorage.util.KeyColumn;

/**
 * Threadsafe discretionary locking within and between processes JanusGraph.
 * <p>
 * Locks are identified {@link KeyColumn} instances.
 * {@code KeyColumn#equals(Object)} should be used to determine whether two
 * {@code KeyColumn} instances refer to the same or different locks.
 * <p>
 * Threads taking locks are identified by {@link StoreTransaction} instances.
 * Either {@code equals(...)} or reference equality (the {@code ==} operator)
 * can be used to compare these instances.
 * <p>
 * This interface follows a three-step locking model that supports both
 * nonblocking and blocking locking primitives. Not all JanusGraph
 * {@code StoreTransaction}s will need locks and use this interface. For those
 * {@code StoreTransaction}s that do take one or more locks, JanusGraph will call the
 * methods on this interface in the following order. {@code tx} refers to the
 * same {@code StoreTransaction} instance in each step.
 * 
 * <ol>
 * <li>{@code writeLock(kc, tx)} one or more times per distinct
 * {@code KeyColumn kc}
 * <li>{@code checkLocks(tx)} once if the user commits {@code tx} or never if
 * the user aborts {@code tx}
 * <li>{@code deleteLocks(tx)} once, regardless of whether
 * {@code checkLocks(tx)} was called
 * </ol>
 * 
 * <p>
 * The first step, {@code writeLocks}, attempts to take a lock. This may, but
 * need not necessarily, indicate whether the attempt succeeded or failed. The
 * second step, {@code checkLocks}, returns if all previous {@code writeLocks}
 * succeeded or throws an exception if one or more failed. The final step,
 * {@code deleteLocks}, releases all locks and associated resources held by the
 * {@code tx}.
 * <p>
 * All implementations of this interface must be safe for concurrent use by
 * different threads. However, implementations may assume that, for any given
 * {@code StoreTransaction tx}, all calls to this interface's methods with
 * argument {@code tx} will either come from a single thread or multiple threads
 * using external synchronization to provide the same effect.
 */
public interface Locker {

    /**
     * Attempt to acquire/take/claim/write the lock named by {@code lockID}.
     * <p>
     * Returns on success and throws an exception on failure.
     * 
     * @param lockID
     *            the lock to acquire
     * @param tx
     *            the transaction attempting to acquire the lock
     * @throws TemporaryLockingException
     *             a failure likely to disappear if the call is retried
     * @throws PermanentLockingException
     *             a failure unlikely to disappear if the call is retried
     */
    void writeLock(KeyColumn lockID, StoreTransaction tx)
            throws TemporaryLockingException, PermanentLockingException;

    /**
     * Verify that all previous {@link #writeLock(KeyColumn, StoreTransaction)}
     * calls with {@code tx} actually succeeded.
     * <p>
     * Returns on success and throws an exception on failure.
     *
     * @param tx
     *            the transaction attempting to check the result of previous
     *            {@code writeLock(..., tx)} calls in which it was the
     *            {@code tx} argument
     * @throws TemporaryLockingException
     *             a failure likely to disappear if the call is retried
     * @throws PermanentLockingException
     *             a failure unlikely to disappear if the call is retried
     */
    void checkLocks(StoreTransaction tx)
            throws TemporaryLockingException, PermanentLockingException;

    /**
     * Release every lock currently held by {@code tx}.
     * <p>
     * Returns on success and throws an exception on failure.
     *
     * @param tx
     *            the transaction attempting to delete locks taken in previous
     *            {@code writeLock(..., tx)} calls in which it was the
     *            {@code tx} argument
     * @throws TemporaryLockingException
     *             a failure likely to disappear if the call is retried
     * @throws PermanentLockingException
     *             a failure unlikely to disappear if the call is retried
     */
    void deleteLocks(StoreTransaction tx)
            throws TemporaryLockingException, PermanentLockingException;
}
