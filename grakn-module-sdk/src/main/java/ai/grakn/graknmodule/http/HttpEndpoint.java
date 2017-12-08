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
 * A class representing an HTTP endpoint supported by a {@link ai.grakn.graknmodule.GraknModule}
 *
 * @author Ganeshwara Herawan Hananda
 */
public interface HttpEndpoint {
    /**
     * Specifies the HTTP method supported by this endpoint
     * @return A {@link ai.grakn.graknmodule.http.HttpMethods.HTTP_METHOD}
     */
    HttpMethods.HTTP_METHOD getHttpMethod();

    /**
     * The URL for this endpoint
     * @return The endpoint's URL
     */
    String getEndpoint();

    /**
     * The request handler for this endpoint
     * @return A {@link HttpResponse}
     */
    HttpResponse getRequestHandler(HttpRequest request);
}
