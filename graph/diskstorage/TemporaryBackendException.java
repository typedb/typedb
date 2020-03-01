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
 * This exception signifies a (potentially) temporary exception in a JanusGraph storage backend,
 * that is, an exception that is due to a temporary unavailability or other exception that
 * is not permanent in nature.
 * <p>
 * If this exception is thrown it indicates that retrying the same operation might potentially
 * lead to success (but not necessarily)
 * <p>

 */

public class TemporaryBackendException extends BackendException {

    private static final long serialVersionUID = 9286719478969781L;

    public TemporaryBackendException(String msg, boolean enableStacktrace){
        super(msg, null, false, enableStacktrace);
    }

    /**
     * @param msg Exception message
     */
    public TemporaryBackendException(String msg) {
        super(msg);
    }

    /**
     * @param msg   Exception message
     * @param cause Cause of the exception
     */
    public TemporaryBackendException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructs an exception with a generic message
     *
     * @param cause Cause of the exception
     */
    public TemporaryBackendException(Throwable cause) {
        this("Temporary failure in storage backend", cause);
    }


}
