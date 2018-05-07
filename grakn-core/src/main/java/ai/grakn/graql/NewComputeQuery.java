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
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.exception.GraqlQueryException;

import javax.annotation.CheckReturnValue;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static ai.grakn.util.GraqlSyntax.Compute.Algorithm;
import static ai.grakn.util.GraqlSyntax.Compute.Argument;
import static ai.grakn.util.GraqlSyntax.Compute.Method;
import static ai.grakn.util.GraqlSyntax.Compute.Parameter;

/**
 * Graql Compute Query: to perform distributed analytics OLAP computation on Grakn
 *
 * @author Haikal Pribadi
 */
public interface NewComputeQuery extends Query<NewComputeQuery.Answer> {

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
    NewComputeQuery from(ConceptId fromID);

    /**
     * @return a String representing the name of the compute query method
     */
    Method method();

    /**
     * @return a Concept ID in which which compute query will start from
     */
    @CheckReturnValue
    Optional<ConceptId> from();

    /**
     * @param toID is the Concept ID in which compute query will stop at
     * @return A NewComputeQuery with the toID set
     */
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
    NewComputeQuery of(String type, String... types);

    /**
     * @param types is an array of concept types in which the compute query would apply to
     * @return a NewComputeQuery with the of set
     */
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
    NewComputeQuery in(String type, String... types);

    /**
     * @param types is an array of concept types that determines the scope of graph for the compute query
     * @return a NewComputeQuery with the inTypes set
     */
    NewComputeQuery in(Collection<Label> types);

    /**
     * @return the collection of concept types that determines the scope of graph for the compute query
     */
    @CheckReturnValue
    Optional<Set<Label>> in();

    /**
     * @param algorithm name as an condition for the compute query
     * @return a NewComputeQuery with algorithm condition set
     */
    NewComputeQuery using(Algorithm algorithm);

    /**
     * @return the algorithm type for the compute query
     */
    @CheckReturnValue
    Optional<Algorithm> using();

    /**
     * @param arg  is an argument that could provided to modify the compute query parameters
     * @param args is an array of arguments
     * @return a NewComputeQuery with the arguments set
     */
    NewComputeQuery where(Argument arg, Argument... args);

    /**
     * @param args is a list of arguments that could be provided to modify the compute query parameters
     * @return
     */
    NewComputeQuery where(Collection<Argument> args);

    /**
     * @return an Arguments object containing all the provided individual arguments combined
     */
    @CheckReturnValue
    Optional<Arguments> where();

    /**
     * Allow analytics query to include attributes and their relationships
     *
     * @return a ComputeQuery with the inTypes set
     */
    NewComputeQuery includeAttributes(boolean include);

    /**
     * Get if this query will include attributes and their relationships
     */
    @CheckReturnValue
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


    /**
     * Argument inner interface to provide access Compute Query arguments
     *
     * @author Haikal Pribadi
     */
    interface Arguments {

        @CheckReturnValue
        Optional<?> getArgument(Parameter param);

        @CheckReturnValue
        Collection<Parameter> getParameters();

        @CheckReturnValue
        Optional<Long> minK();

        @CheckReturnValue
        Optional<Long> k();

        @CheckReturnValue
        Optional<Long> size();

        @CheckReturnValue
        Optional<Boolean> members();

        @CheckReturnValue
        Optional<ConceptId> contains();
    }

    /**
     * Answer inner interface to provide access to Compute Query computation results
     *
     * @author Haikal Pribadi
     */
    interface Answer {

        @CheckReturnValue
        Optional<Number> getNumber();

        @CheckReturnValue
        Optional<List<List<Concept>>> getPaths();

        @CheckReturnValue
        Optional<Map<Long, Set<String>>> getCentralityCount();

        @CheckReturnValue
        Optional<Map<String, Set<String>>> getClusterMembers();

        @CheckReturnValue
        Optional<Map<String, Long>> getClusterSizes();
    }
}
