/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

import ai.grakn.Keyspace;
import ai.grakn.concept.Concept;

import java.net.URI;

import static ai.grakn.util.ErrorMessage.BACKEND_EXCEPTION;
import static ai.grakn.util.ErrorMessage.COULD_NOT_REACH_ENGINE;
import static ai.grakn.util.ErrorMessage.ENGINE_STARTUP_ERROR;
import static ai.grakn.util.ErrorMessage.INITIALIZATION_EXCEPTION;

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
 * @author fppt
 */
public class GraknBackendException extends GraknException {

    protected GraknBackendException(String error, Exception e) {
        super(error, e);
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

    /**
     * Thrown when engine cannot be reached.
     */
    public static GraknBackendException cannotReach(URI uri){
        return create(COULD_NOT_REACH_ENGINE.getMessage(uri));
    }

    public static GraknBackendException serverStartupException(String message, Exception e){
        return new GraknBackendException(ENGINE_STARTUP_ERROR.getMessage(message), e);
    }

    /**
     * Thrown when trying to convert a {@link Concept} into a response object and failing to do so.
     */
    public static GraknBackendException convertingUnknownConcept(Concept concept){
        return create(String.format("Cannot convert concept {%s} into response object due to it being of an unknown base type", concept));
    }

    public static GraknBackendException initializationException(Keyspace keyspace) {
        return create(INITIALIZATION_EXCEPTION.getMessage(keyspace));
    }

    public static GraknBackendException noSuchKeyspace(Keyspace keyspace) {
        return create("No such keyspace " + keyspace);
    }

    /**
     * Thrown when there is a migration failure due to a backend failure
     */
    public static GraknBackendException migrationFailure(String exception){
        return create("Error on backend has stopped migration: " + exception);
    }
}
