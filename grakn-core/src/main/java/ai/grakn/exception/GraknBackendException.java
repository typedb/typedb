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

package ai.grakn.exception;

import static ai.grakn.util.ErrorMessage.BACKEND_EXCEPTION;

/**
 * <p>
 *     Backend Grakn Exception
 * </p>
 *
 * <p>
 *     Failures which occur server side are wrapped in this exception. This can include but is not limited to:
 *     - Kafka timeouts
 *     - Cassandra Timeouts
 *     - Malformed Requests
 * </p>
 *
 * @author fppt
 */
public class GraknBackendException extends GraknException {
    protected GraknBackendException(String error, Exception e) {
        super(error, e);
    }

    /**
     * Thrown when the persistence layer throws an unexpected exception.
     * This can include timeouts
     */
    public static GraknBackendException unknown(Exception e){
        return new GraknBackendException(BACKEND_EXCEPTION.getMessage(), e);
    }
}
