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

import ai.grakn.Keyspace;
import ai.grakn.engine.TaskId;
import ai.grakn.util.SimpleURI;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.io.IOException;
import java.net.URI;

import static ai.grakn.util.ErrorMessage.BACKEND_EXCEPTION;
import static ai.grakn.util.ErrorMessage.COULD_NOT_REACH_ENGINE;
import static ai.grakn.util.ErrorMessage.ENGINE_STARTUP_ERROR;
import static ai.grakn.util.ErrorMessage.ENGINE_UNAVAILABLE;
import static ai.grakn.util.ErrorMessage.INITIALIZATION_EXCEPTION;
import static ai.grakn.util.ErrorMessage.MISSING_TASK_ID;
import static ai.grakn.util.ErrorMessage.STATE_STORAGE_ERROR;
import static ai.grakn.util.ErrorMessage.TASK_STATE_RETRIEVAL_FAILURE;

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

    protected GraknBackendException(String error){
        super(error);
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
        return new GraknBackendException(COULD_NOT_REACH_ENGINE.getMessage(uri));
    }

    public static GraknBackendException serverStartupException(String message, Exception e){
        return new GraknBackendException(ENGINE_STARTUP_ERROR.getMessage(message), e);
    }

    /**
     * Thrown when the task state storage cannot be accessed.
     */
    public static GraknBackendException stateStorage(){
        return new GraknBackendException(STATE_STORAGE_ERROR.getMessage());
    }

    /**
     * Thrown when a task id is missing from the task state storage
     */
    public static GraknBackendException stateStorageMissingId(TaskId id){
        return new GraknBackendException(MISSING_TASK_ID.getMessage(id));
    }

    /**
     * Thrown when a task id is missing from the task state storage
     */
    public static GraknBackendException stateStorageTaskRetrievalFailure(Exception e){
        return new GraknBackendException(TASK_STATE_RETRIEVAL_FAILURE.getMessage(ExceptionUtils.getFullStackTrace(e)));
    }

    /**
     * Thrown when the task client cannot reach engine
     */
    public static GraknBackendException engineUnavailable(SimpleURI uri, IOException e){
        return new GraknBackendException(ENGINE_UNAVAILABLE.getMessage(uri), e);
    }

    public static GraknBackendException initializationException(Keyspace keyspace) {
        return new GraknBackendException(INITIALIZATION_EXCEPTION.getMessage(keyspace));
    }

    public static GraknBackendException noSuchKeyspace(Keyspace keyspace) {
        return new GraknBackendException("No such keyspace " + keyspace);
    }

    /**
     * Thrown when there is a migration failure due to a backend failure
     */
    public static GraknBackendException migrationFailure(String exception){
        return new GraknBackendException("Error on backend has stopped migration: " + exception);
    }
}
