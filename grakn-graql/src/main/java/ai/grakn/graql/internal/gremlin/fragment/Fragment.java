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

package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.graql.VarName;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Optional;
import java.util.Set;

/**
 * represents a graph traversal, with one start point and optionally an end point
 * <p>
 * A fragment is composed of four things:
 * <ul>
 *     <li>A gremlin traversal function, that takes a gremlin traversal and appends some new gremlin steps</li>
 *     <li>A starting variable name, where the gremlin traversal must start from</li>
 *     <li>An optional ending variable name, if the gremlin traversal navigates to a new Graql variable</li>
 *     <li>A priority, that describes how efficient this traversal is to help with ordering the traversals</li>
 * </ul>
 *
 * Variable names refer to Graql variables. Some of these variable names may be randomly-generated UUIDs, such as for
 * castings.
 * <p>
 * A {@code Fragment} is usually contained in a {@code EquivalentFragmentSet}, which contains multiple fragments describing
 * the different directions the traversal can be followed in, with different starts and ends.
 * <p>
 * A gremlin traversal is created from a {@code Query} by appending together fragments in order of priority, one from
 * each {@code EquivalentFragmentSet} describing the {@code Query}.
 *
 * @author Felix Chapman
 */
public interface Fragment {

    /**
     * @return the EquivalentFragmentSet that contains this Fragment
     */
    EquivalentFragmentSet getEquivalentFragmentSet();

    /**
     * @param equivalentFragmentSet the EquivalentFragmentSet that contains this Fragment
     */
    void setEquivalentFragmentSet(EquivalentFragmentSet equivalentFragmentSet);

    /**
     * @param traversal the traversal to extend with this Fragment
     */
    void applyTraversal(GraphTraversal<Vertex, Vertex> traversal);

    /**
     * The name of the fragment
     */
    String getName();

    /**
     * @return the variable name that this fragment starts from in the query
     */
    VarName getStart();

    /**
     * @return the variable name that this fragment ends at in the query, if this query has an end variable
     */
    Optional<VarName> getEnd();

    /**
     * @return the variable names that this fragment requires to have already been visited
     */
    Set<VarName> getDependencies();

    /**
     * Get all variable names in the fragment - the start and end (if present)
     */
    Set<VarName> getVariableNames();

    /**
     * A starting fragment is a fragment that can start a traversal.
     * If any other fragment is present that refers to the same variable, the starting fragment can be omitted.
     */
    default boolean isStartingFragment() {
        return false;
    }

    double fragmentCost(double previousCost);
}
