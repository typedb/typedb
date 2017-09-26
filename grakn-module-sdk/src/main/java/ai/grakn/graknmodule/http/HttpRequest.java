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
 *
 */

package ai.grakn.graknmodule.http;

import java.util.Map;

/**
 * A class representing an HTTP request
 *
 * @author Ganeshwara Herawan Hananda
 */
public class HttpRequest {
    private final Map<String, String> headers;
    private final Map<String, String> queryParameters;
    private final String requestBody;

    public HttpRequest(Map<String, String> headers, Map<String, String> queryParameters, String requestBody) {
        this.headers = headers;
        this.queryParameters = queryParameters;
        this.requestBody = requestBody;
    }

    /**
     * Returns the HTTP headers as a {@link Map}
     * @return HTTP headers
     */
    public Map<String, String> getHeaders() {
        return headers;
    }

    /**
     * Returns the query parameters as a {@link Map}
     * @return HTTP query parameters
     */
    public Map<String, String> getQueryParameters() {
        return queryParameters;
    }

    /**
     * Returns the request body
     * @return HTTP request body
     */
    public String getRequestBody() {
        return requestBody;
    }
}
