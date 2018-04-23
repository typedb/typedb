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

package ai.grakn.graql.analytics;

import ai.grakn.GraknTx;
import ai.grakn.concept.Label;
import ai.grakn.graql.Query;

import javax.annotation.CheckReturnValue;
import java.util.Collection;

/**
 * A query that triggers an analytics OLAP computation on a graph.
 *
 * @param <T> the type of result this query will return
 * @author Jason Liu
 */
public interface ComputeQuery<T> extends Query<T> {

    /**
     * @param tx the graph to execute the compute query on
     * @return a ComputeQuery with the graph set
     */
    @Override
    ComputeQuery<T> withTx(GraknTx tx);

    /**
     * @param subTypelabels an array of types to include in the subgraph
     * @return a ComputeQuery with the subTypelabels set
     */
    @CheckReturnValue
    ComputeQuery<T> in(String... subTypelabels);

    /**
     * @param subLabels a collection of types to include in the subgraph
     * @return a ComputeQuery with the inTypes set
     */
    @CheckReturnValue
    ComputeQuery<T> in(Collection<? extends Label> subLabels);

    /**
     * Get the collection of types to include in the subgraph
     */
    Collection<? extends Label> inTypes();

    /**
     * Allow analytics query to include attributes and their relationships
     *
     * @return a ComputeQuery with the inTypes set
     */
    @CheckReturnValue
    ComputeQuery<T> includeAttribute();

    /**
     * Get if this query will include attributes and their relationships
     */
    boolean isAttributeIncluded();

    /**
     * Whether this query will modify the graph
     */
    @Override
    default boolean isReadOnly() {
        return true;
    }

    /**
     * kill the compute query, terminate the job
     */
    void kill();
}
