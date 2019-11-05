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

import grakn.core.graph.diskstorage.locking.consistentkey.ConsistentKeyLockStatus;

import java.time.Instant;

/**
 * A single held lock's expiration time. This is used by {@link AbstractLocker}.
 *
 * @see AbstractLocker
 * @see ConsistentKeyLockStatus
 */
public interface LockStatus {

    /**
     * Returns the moment at which this lock expires (inclusive).
     *
     * @return The expiration instant of this lock
     */
    Instant getExpirationTimestamp();
}
