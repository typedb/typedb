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

package ai.grakn.graql;

import ai.grakn.GraknTx;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.graql.analytics.ComputeQuery;

import javax.annotation.CheckReturnValue;
import java.util.Collection;
import java.util.Optional;

/**
 * Graql Compute Query: to perform distributed analytics OLAP computation on Grakn.
 *
 * @author Haikal Pribadi
 */
public interface NewComputeQuery extends ComputeQuery<ComputeAnswer> {

    /**
     * @param tx the graph to execute the compute query on
     * @return a ComputeQuery with the graph set
     */
    @Override
    NewComputeQuery withTx(GraknTx tx);

    @CheckReturnValue
    NewComputeQuery from(ConceptId fromID);

    @CheckReturnValue
    Optional<ConceptId> from();

    @CheckReturnValue
    NewComputeQuery to(ConceptId toID);

    @CheckReturnValue
    Optional<ConceptId> to();

    /**
     * @param inTypes an array of types to include in the subgraph
     * @return a ComputeQuery with the inTypes set
     */
    @CheckReturnValue
    NewComputeQuery in(String... inTypes);

    /**
     * @param inTypes a collection of types to include in the subgraph
     * @return a ComputeQuery with the inTypes set
     */
    @CheckReturnValue
    NewComputeQuery in(Collection<? extends Label> inTypes);

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
    NewComputeQuery includeAttribute();

    /**
     * Get if this query will include attributes and their relationships
     */
    boolean isAttributeIncluded();

    /**
     * Checks whether this query is a valid Graql Compute query given the provided conditions
     */
    @CheckReturnValue
    boolean isValid();

    /**
     * Checks Whether this query will modify the graph
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
