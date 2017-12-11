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
 * A class representing an HTTP endpoint before filter, supported by a {@link ai.grakn.graknmodule.GraknModule}
 *
 * @author Ganeshwara Herawan Hananda
 */

public interface HttpBeforeFilter {
    /**
     * @return the URL pattern of which this HttpBeforeFilter will be applied on
     */
    String getUrlPattern();

    /**
     * The filter which will be applied to the endpoints
     * @param Http request object
     * @return {@link HttpBeforeFilterResult} object indicating whether the request should be allowed or denied
     */
    HttpBeforeFilterResult getHttpBeforeFilter(HttpRequest request);
}
