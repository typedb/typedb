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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.engine.controller.util;

import ai.grakn.engine.controller.response.Link;
import ai.grakn.exception.GraknServerException;
import mjson.Json;
import spark.Request;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;

/**
 * Utility class for handling http requests
 * @author Domenico Corapi
 */
public class Requests {
    /**
     * Given a {@link Request} object retrieve the value of the {@param parameter} argument. If it is not present
     * in the request query, return a 400 to the client.
     *
     * @param request information about the HTTP request
     * @param parameter value to retrieve from the HTTP request
     * @return value of the given parameter
     */
    public static String mandatoryQueryParameter(Request request, String parameter){
        return mandatoryQueryParameter(p -> queryParameter(request, p), parameter);
    }

    /**
     * Given a {@link Function}, retrieve the value of the {@param parameter} by applying that function
     * @param extractParameterFunction function used to extract the parameter
     * @param parameter value to retrieve from the HTTP request
     * @return value of the given parameter
     */
    public static <T> T mandatoryQueryParameter(Function<String, Optional<T>> extractParameterFunction, String parameter) {
        return extractParameterFunction.apply(parameter).orElseThrow(() ->
                GraknServerException.requestMissingParameters(parameter));
    }

    /**
     * Given a {@link Request}, retrieve the value of the {@param parameter}
     * @param request information about the HTTP request
     * @param parameter value to retrieve from the HTTP request
     * @return value of the given parameter
     */
    public static Optional<String> queryParameter(Request request, String parameter){
        return Optional.ofNullable(request.queryParams(parameter));
    }


    /**
     * Given a {@link Request), retreive the value of the request body. If the request does not have a body,
     * return a 400 (missing parameter) to the client.
     *
     * @param request information about the HTTP request
     * @return value of the request body as a string
     */
    public static String mandatoryBody(Request request){
        return Optional.ofNullable(request.body()).filter(s -> !s.isEmpty()).orElseThrow(
                GraknServerException::requestMissingBody);
    }

    public static Link selfLink(Request request) {
        return Link.create(request.pathInfo());
    }

    /**
     * Given a {@link Function}, retrieve the value of the {@param parameter} by applying that function
     * @param parameter value to retrieve from the HTTP request
     * @return value of the given parameter
     */

    public static String mandatoryPathParameter(Request request, String parameter) {
        // TODO: add new method GraknServerException.requestMissingPathParameters
        String parameterValue = Optional.ofNullable(request.params(parameter)).orElseThrow(() ->
            GraknServerException.requestMissingParameters(parameter));

        return parameterValue;
    }

    /**
     * Given a {@link Json} object, attempt to extract a single field as supplied,
     * or throw a user-friendly exception clearly indicating the missing field
     * @param json the {@link Json} object containing the field to be extracted
     * @param fieldPath String varargs representing the path of the field to be extracted
     * @return the extracted {@link Json} object
     * @throws {@link GraknServerException} with a clear indication of the missing field
     */
    public static Json extractJsonField(Json json, String... fieldPath) {
        Json currentField = json;

        for (String field : fieldPath) {
            Json tmp = currentField.at(field);
            if (tmp != null) {
                currentField = tmp;
            } else {
                throw GraknServerException.requestMissingBodyParameters(field);
            }
        }

        return currentField;
    }

    /**
     * Checks that the Request is of the valid type
     *
     * @param request
     * @param contentTypes
     */
    public static void validateRequest(Request request, String... contentTypes){
        String acceptType = getAcceptType(request);

        if(!Arrays.asList(contentTypes).contains(acceptType)){
            throw GraknServerException.unsupportedContentType(acceptType);
        }
    }

    /**
     * Gets the accepted type of the request
     *
     * @param request
     * @return
     */
    public static String getAcceptType(Request request) {
        // TODO - we are not handling multiple values here and we should!
        String header = request.headers("Accept");
        return header == null ? "" : request.headers("Accept").split(",")[0];
    }
}
