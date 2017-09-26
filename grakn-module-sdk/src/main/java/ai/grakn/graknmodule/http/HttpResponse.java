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

/**
 * A class representing an HTTP response
 *
 * @author Ganeshwara Herawan Hananda
 */

public class HttpResponse {
    public HttpResponse(int statusCode, String body) {
        this.statusCode = statusCode;
        this.body = body;
    }

    /**
     * Get the HTTP response status code
     * @return HTTP response status code
     */
    public int getStatusCode() {
        return statusCode;
    }

    /**
     * Get the body out of the HTTP response
     * @return HTTP response body
     */
    public String getBody() {
        return body;
    }

    private final int statusCode;
    private final String body;
}
