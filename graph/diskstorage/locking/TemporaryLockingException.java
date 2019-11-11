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

import grakn.core.graph.diskstorage.TemporaryBackendException;

/**
 * This exception signifies a (potentially) temporary exception while attempting
 * to acquire a lock in the JanusGraph storage backend. These can occur due to
 * request timeouts, network failures, etc. Temporary failures represented by
 * this exception might disappear if the request is retried, even if no machine
 * modifies the underlying lock state between the failure and follow-up request.
 * <p>
 * 

 */

public class TemporaryLockingException extends TemporaryBackendException {

    private static final long serialVersionUID = 482890657293484420L;

    /**
     * @param msg Exception message
     */
    public TemporaryLockingException(String msg) {
        super(msg);
    }

    /**
     * @param msg   Exception message
     * @param cause Cause of the exception
     */
    public TemporaryLockingException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructs an exception with a generic message
     *
     * @param cause Cause of the exception
     */
    public TemporaryLockingException(Throwable cause) {
        this("Temporary locking failure", cause);
    }


}
