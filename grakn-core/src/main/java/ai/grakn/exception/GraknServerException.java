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
import ai.grakn.concept.ConceptId;
import ai.grakn.graql.Query;

import static ai.grakn.util.ErrorMessage.AUTHENTICATION_FAILURE;
import static ai.grakn.util.ErrorMessage.CANNOT_DELETE_KEYSPACE;
import static ai.grakn.util.ErrorMessage.ENGINE_ERROR;
import static ai.grakn.util.ErrorMessage.EXPLAIN_ONLY_MATCH;
import static ai.grakn.util.ErrorMessage.INVALID_CONTENT_TYPE;
import static ai.grakn.util.ErrorMessage.INVALID_QUERY_USAGE;
import static ai.grakn.util.ErrorMessage.MISSING_MANDATORY_BODY_REQUEST_PARAMETERS;
import static ai.grakn.util.ErrorMessage.MISSING_MANDATORY_REQUEST_PARAMETERS;
import static ai.grakn.util.ErrorMessage.MISSING_REQUEST_BODY;
import static ai.grakn.util.ErrorMessage.NO_CONCEPT_IN_KEYSPACE;
import static ai.grakn.util.ErrorMessage.UNAVAILABLE_TASK_CLASS;
import static ai.grakn.util.ErrorMessage.UNSUPPORTED_CONTENT_TYPE;
import static org.apache.http.HttpStatus.SC_BAD_REQUEST;
import static org.apache.http.HttpStatus.SC_INTERNAL_SERVER_ERROR;
import static org.apache.http.HttpStatus.SC_METHOD_NOT_ALLOWED;
import static org.apache.http.HttpStatus.SC_NOT_ACCEPTABLE;
import static org.apache.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.http.HttpStatus.SC_UNAUTHORIZED;

/**
 * <p>
 *     Grakn Server Exception
 * </p>
 *
 * <p>
 *     Wraps backend exception which require a status code which needs to be returned to the client.
 * </p>
 *
 * @author fppt
 */
public class GraknServerException extends GraknBackendException {
    private final int status;

    private GraknServerException(String error, Exception e, int status) {
        super(error, e);
        this.status = status;
    }

    private GraknServerException(String error, int status){
        super(error);
        this.status = status;
    }

    /**
     * Gets the error status code if one is available
     */
    public int getStatus(){
        return status;
    }

    /**
     * Thrown when the Grakn server has an internal exception.
     * This is thrown upwards to be interpreted by clients
     */
    public static GraknServerException serverException(int status, Exception e){
        return new GraknServerException(ENGINE_ERROR.getMessage(), e, status);
    }

    /**
     * Thrown when attempting to create an invalid task
     */
    public static GraknServerException invalidTask(String className){
        return new GraknServerException(UNAVAILABLE_TASK_CLASS.getMessage(className), SC_BAD_REQUEST);
    }

    /**
     * Thrown when a request has an invalid parameter
     */
    public static GraknServerException requestInvalidParameter(String parameter, String value){
        String message = String.format("Invalid value %s for parameter %s", value, parameter);
        return new GraknServerException(message, SC_BAD_REQUEST);
    }

    /**
     * Thrown when a request is missing mandatory parameters
     */
    public static GraknServerException requestMissingParameters(String parameter){
        return new GraknServerException(MISSING_MANDATORY_REQUEST_PARAMETERS.getMessage(parameter), SC_BAD_REQUEST);
    }

    /**
     * Thrown when a request is missing mandatory parameters in the body
     */
    public static GraknServerException requestMissingBodyParameters(String parameter){
        return new GraknServerException(
                MISSING_MANDATORY_BODY_REQUEST_PARAMETERS.getMessage(parameter), SC_BAD_REQUEST);
    }

    /**
     * Thrown when a request is missing the body
     */
    public static GraknServerException requestMissingBody(){
        return new GraknServerException(MISSING_REQUEST_BODY.getMessage(), SC_BAD_REQUEST);
    }

    /**
     * Thrown the content type specified in a request is invalid
     */
    public static GraknServerException unsupportedContentType(String contentType){
        return new GraknServerException(UNSUPPORTED_CONTENT_TYPE.getMessage(contentType), SC_NOT_ACCEPTABLE);
    }

    /**
     * Thrown when there is a mismatch between the content type in the request and the query to be executed
     */
    public static GraknServerException contentTypeQueryMismatch(String contentType, Query query){
        return new GraknServerException(
                INVALID_CONTENT_TYPE.getMessage(query.getClass().getName(), contentType), SC_NOT_ACCEPTABLE);
    }

    /**
     * Thrown when an incorrect query is used with a REST endpoint
     */
    public static GraknServerException invalidQuery(String queryType){
        return new GraknServerException(INVALID_QUERY_USAGE.getMessage(queryType), SC_METHOD_NOT_ALLOWED);
    }

    /**
     * Thrown when asked to explain a non-get query
     */
    public static GraknServerException invalidQueryExplaination(String query){
        return new GraknServerException(EXPLAIN_ONLY_MATCH.getMessage(query), SC_METHOD_NOT_ALLOWED);
    }

    /**
     * Thrown when an incorrect query is used with a REST endpoint
     */
    public static GraknServerException authenticationFailure(){
        return new GraknServerException(AUTHENTICATION_FAILURE.getMessage(), SC_UNAUTHORIZED);
    }

    /**
     * Thrown when engine cannot delete a keyspace as expected
     */
    public static GraknServerException couldNotDelete(Keyspace keyspace){
        return new GraknServerException(CANNOT_DELETE_KEYSPACE.getMessage(keyspace), SC_INTERNAL_SERVER_ERROR);
    }

    /**
     * Thrown when requested concept is not found in the graph
     */
    public static GraknServerException noConceptFound(ConceptId conceptId, Keyspace keyspace){
        return new GraknServerException(NO_CONCEPT_IN_KEYSPACE.getMessage(conceptId, keyspace), SC_NOT_FOUND);
    }

    /**
     * Thrown when an internal server error occurs. This is likely due to incorrect configs
     */
    public static GraknServerException internalError(String errorMessage){
        return new GraknServerException(errorMessage, SC_INTERNAL_SERVER_ERROR);
    }
}
