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
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.analytics.ComputeQuery;

import javax.annotation.CheckReturnValue;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

/**
 * Graql Compute Query: to perform distributed analytics OLAP computation on Grakn.
 *
 * @author Haikal Pribadi
 */
public interface NewComputeQuery extends Query<ComputeAnswer> {

    /**
     * @param tx the graph to execute the compute query on
     * @return a ComputeQuery with the graph set
     */
    @Override
    NewComputeQuery withTx(GraknTx tx);

    /**
     * @param fromID is the Concept ID of in which compute path query will start from
     * @return A NewComputeQuery with the fromID set
     */
    @CheckReturnValue
    NewComputeQuery from(ConceptId fromID);

    /**
     * @return a String representing the name of the compute query method
     */
    String method();
    /**
     * @return a Concept ID in which which compute query will start from
     */
    @CheckReturnValue
    Optional<ConceptId> from();

    /**
     * @param toID is the Concept ID in which compute query will stop at
     * @return A NewComputeQuery with the toID set
     */
    @CheckReturnValue
    NewComputeQuery to(ConceptId toID);

    /**
     * @return a Concept ID in which which compute query will stop at
     */
    @CheckReturnValue
    Optional<ConceptId> to();

    /**
     * @param types is an array of concept types in which the compute query would apply to
     * @return a NewComputeQuery with the of set
     */
    @CheckReturnValue
    NewComputeQuery of(String... types);

    /**
     * @param types is an array of concept types in which the compute query would apply to
     * @return a NewComputeQuery with the of set
     */
    @CheckReturnValue
    NewComputeQuery of(Collection<Label> types);

    /**
     * @return the collection of concept types in which the compute query would apply to
     */
    @CheckReturnValue
    Optional<Set<Label>> of();

    /**
     * @param types is an array of concept types that determines the scope of graph for the compute query
     * @return a NewComputeQuery with the inTypes set
     */
    @CheckReturnValue
    NewComputeQuery in(String... types);

    /**
     * @param types is an array of concept types that determines the scope of graph for the compute query
     * @return a NewComputeQuery with the inTypes set
     */
    @CheckReturnValue
    NewComputeQuery in(Collection<Label> types);

    /**
     * @return the collection of concept types that determines the scope of graph for the compute query
     */
    @CheckReturnValue
    Optional<Set<Label>> in();

    /**
     * @param algorithm name as an argument for the compute query
     * @return a NewComputeQuery with algorithm argument set
     */
    NewComputeQuery using(String algorithm);

    /**
     * @return the algorithm argument for the compute query
     */
    Optional<String> using();

    /**
     * @param minK is a number representing the minimum K (aka. min-k) value as an argument for the compute query
     * @return a NewComputeQuery with the min-k argument set
     */
    NewComputeQuery minK(long minK);

    /**
     * @return the min-k argument for the compute query
     */
    @CheckReturnValue
    Optional<Long> minK();

    /**
     * Allow analytics query to include attributes and their relationships
     *
     * @return a ComputeQuery with the inTypes set
     */
    @CheckReturnValue
    NewComputeQuery includeAttributes(boolean include);

    /**
     * Get if this query will include attributes and their relationships
     */
    boolean includesAttributes();

    /**
     * @return a boolean representing whether this query is a valid Graql Compute query given the provided conditions
     */
    @CheckReturnValue
    boolean isValid();

    /**
     * @return any exception if the query is invalid
     */
    @CheckReturnValue
    Optional<GraqlQueryException> getException();

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
