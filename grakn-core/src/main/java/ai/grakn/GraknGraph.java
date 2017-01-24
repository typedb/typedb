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

package ai.grakn;

import ai.grakn.exception.GraknValidationException;
import ai.grakn.graph.BaseGraknGraph;

/**
 * <p>
 *     A Grakn Graph
 * </p>
 *
 * <p>
 *     This is produced by {@link Grakn#factory(String, String)} and allows the user to construct and perform
 *     basic look ups to a Grakn Graph. This also allows the execution of Graql queries.
 * </p>
 *
 * @author fppt
 *
 */
public interface GraknGraph extends BaseGraknGraph {

    /**
     * Validates and attempts to commit the graph. Also submits commit logs for post processing
     * An exception is thrown if validation fails or if the graph cannot be persisted due to an underlying database issue.
     *
     * @throws GraknValidationException is thrown when a structural validation fails.
     */
    void commit() throws GraknValidationException;
}
