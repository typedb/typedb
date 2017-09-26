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

package ai.grakn.graknmodule;

import ai.grakn.graknmodule.http.HttpEndpoint;
import ai.grakn.graknmodule.http.HttpBeforeFilter;

import java.util.List;

/**
 * An interface for author who wants to implement a Grakn module
 *
 * @author Ganeshwara Herawan Hananda
 */
public interface GraknModule {
    /**
     * This method must return the ID of the Grakn Module
     * @return the ID of the Grakn Module
     */
    String getGraknModuleName();

    /**
     * This method should return the list of {@link HttpBeforeFilter} objects, which are to be applied to Grakn HTTP endpoints
     * @return list of {@link HttpBeforeFilter}
     */
    List<HttpBeforeFilter> getHttpBeforeFilters();

    /**
     * This method should return the list of {@link HttpEndpoint} objects
     * @return list of {@link HttpEndpoint} object
     */
    List<HttpEndpoint> getHttpEndpoints();
}