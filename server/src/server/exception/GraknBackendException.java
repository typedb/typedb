/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package grakn.core.server.exception;

import grakn.core.server.keyspace.Keyspace;

import static grakn.core.util.ErrorMessage.BACKEND_EXCEPTION;
import static grakn.core.util.ErrorMessage.INITIALIZATION_EXCEPTION;

/**
 * <p>
 *     Backend Grakn Exception
 * </p>
 *
 * <p>
 *     Failures which occur server side are wrapped in this exception. This can include but is not limited to:
 *     - Cassandra Timeouts
 *     - Malformed Requests
 * </p>
 *
 */
public class GraknBackendException extends GraknException {

    private final String NAME = "GraknBackendException";

    protected GraknBackendException(String error, Exception e) {
        super(error, e);
    }

    @Override
    public String getName() {
        return NAME;
    }

    GraknBackendException(String error){
        super(error);
    }

    public static GraknBackendException create(String error) {
        return new GraknBackendException(error);
    }

    /**
     * Thrown when the persistence layer throws an unexpected exception.
     * This can include timeouts
     */
    public static GraknBackendException unknown(Exception e){
        return new GraknBackendException(BACKEND_EXCEPTION.getMessage(), e);
    }

    public static GraknBackendException initializationException(Keyspace keyspace) {
        return create(INITIALIZATION_EXCEPTION.getMessage(keyspace));
    }
}
