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

package ai.grakn.engine.controller.util;

import ai.grakn.exception.GraknServerException;
import java.util.Optional;
import java.util.function.Function;
import spark.Request;

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

    /**
     * Given a {@link Function}, retrieve the value of the {@param parameter} by applying that function
     * @param extractParameterFunction function used to extract the parameter
     * @param parameter value to retrieve from the HTTP request
     * @return value of the given parameter
     */

    public static String mandatoryPathParameter(Request request, String parameter) {
        // TODO: add new method GraknServerException.requestMissingPathParameters
        String parameterValue = Optional.ofNullable(request.params(parameter)).orElseThrow(() ->
            GraknServerException.requestMissingParameters(parameter));

        return parameterValue;
    }
}
