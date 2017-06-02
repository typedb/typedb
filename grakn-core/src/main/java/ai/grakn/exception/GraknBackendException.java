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

import ai.grakn.engine.TaskId;
import ai.grakn.graql.Query;
import ai.grakn.util.ErrorMessage;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.io.IOException;

import static ai.grakn.util.ErrorMessage.AUTHENTICATION_FAILURE;
import static ai.grakn.util.ErrorMessage.BACKEND_EXCEPTION;
import static ai.grakn.util.ErrorMessage.ENGINE_ERROR;
import static ai.grakn.util.ErrorMessage.ENGINE_UNAVAILABLE;
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
 *     - Kafka timeouts
 *     - Cassandra Timeouts
 *     - Malformed Requests
 * </p>
 *
 * @author fppt
 */
public class GraknBackendException extends GraknException {
    private int status = 500; //Assumes most errors are internal

    protected GraknBackendException(String error, Exception e, int status) {
        super(error, e);
        this.status = status;
    }

    protected GraknBackendException(String error, Exception e) {
        super(error, e);
    }

    protected GraknBackendException(String error, int status){
        super(error);
        this.status = status;
    }

    protected GraknBackendException(String error){
        super(error);
    }

    /**
     * Gets the error status code if one is available
     */
    public int getStatus(){
        return status;
    }

    /**
     * Thrown when the persistence layer throws an unexpected exception.
     * This can include timeouts
     */
    public static GraknBackendException unknown(Exception e){
        return new GraknBackendException(BACKEND_EXCEPTION.getMessage(), e);
    }

    /**
     * Thrown when the task state storage cannot be accessed.
     */
    public static GraknBackendException stateStorage(Exception error){
        return new GraknBackendException(STATE_STORAGE_ERROR.getMessage(), error);
    }

    /**
     * Thrown when the task state storage cannot be accessed.
     */
    public static GraknBackendException stateStorage(String error){
        return new GraknBackendException(STATE_STORAGE_ERROR.getMessage() + error);
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
    public static GraknBackendException engineUnavailable(String host, int port, IOException e){
        return new GraknBackendException(ENGINE_UNAVAILABLE.getMessage(host, port), e);
    }

    /**
     * Thrown when the Grakn server has an internal exception.
     * This is thrown upwards to be interpreted by clients
     */
    public static GraknBackendException serverException(int status, Exception e){
        return new GraknBackendException(ENGINE_ERROR.getMessage(), e, status);
    }

    /**
     * Thrown when attempting to create an invalid task
     */
    public static GraknBackendException invalidTask(String className){
        return new GraknBackendException(ErrorMessage.UNAVAILABLE_TASK_CLASS.getMessage(className), 400);
    }

    /**
     * Thrown when a request is missing mandatory parameters
     */
    public static GraknBackendException requestMissingParameters(String parameter){
        return new GraknBackendException(ErrorMessage.MISSING_MANDATORY_REQUEST_PARAMETERS.getMessage(parameter), 400);
    }

    /**
     * Thrown when a request is missing the body
     */
    public static GraknBackendException requestMissingBody(){
        return new GraknBackendException(ErrorMessage.MISSING_REQUEST_BODY.getMessage(), 400);
    }

    /**
     * Thrown the content type specified in a request is invalid
     */
    public static GraknBackendException unsupportedContentType(String contentType){
        return new GraknBackendException(ErrorMessage.UNSUPPORTED_CONTENT_TYPE.getMessage(contentType), 406);
    }

    /**
     * Thrown when there is a mismatch between the content type in the request and the query to be executed
     */
    public static GraknBackendException contentTypeQueryMismatch(String contentType, Query query){
        return new GraknBackendException(ErrorMessage.INVALID_CONTENT_TYPE.getMessage(query.getClass().getName(), contentType), 406);
    }

    /**
     * Thrown when an incorrect query is used with a REST endpoint
     */
    public static GraknBackendException invalidQuery(String queryType){
        return new GraknBackendException(ErrorMessage.INVALID_QUERY_USAGE.getMessage(queryType), 405);
    }

    /**
     * Thrown when asked to explain a non-match query
     */
    public static GraknBackendException invalidQueryExplaination(String query){
        return new GraknBackendException(ErrorMessage.EXPLAIN_ONLY_MATCH.getMessage(query), 405);
    }

    /**
     * Thrown when an incorrect query is used with a REST endpoint
     */
    public static GraknBackendException authenticationFailure(){
        return new GraknBackendException(AUTHENTICATION_FAILURE.getMessage(), 401);
    }

    /**
     * Thrown when an internal server error occurs. This is likely due to incorrect configs
     */
    public static GraknBackendException internalError(String errorMessage){
        return new GraknBackendException(errorMessage, 500);
    }
}
