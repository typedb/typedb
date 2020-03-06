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

/**
 * This exception signifies a permanent exception in a JanusGraph storage backend,
 * that is, an exception that is due to a permanent failure while persisting
 * data.
 * <p>

 */

public class PermanentBackendException extends BackendException {

    private static final long serialVersionUID = 203482308203400L;

    /**
     * @param msg Exception message
     */
    public PermanentBackendException(String msg) {
        super(msg);
    }

    /**
     * @param msg   Exception message
     * @param cause Cause of the exception
     */
    public PermanentBackendException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructs an exception with a generic message
     *
     * @param cause Cause of the exception
     */
    public PermanentBackendException(Throwable cause) {
        this("Permanent failure in storage backend", cause);
    }


}
