/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.gremlin;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Optional;
import java.util.function.UnaryOperator;

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
 * A {@code Fragment} is usually contained in a {@code MultiTraversal}, which contains multiple fragments describing
 * the different directions the traversal can be followed in, with different starts and ends.
 * <p>
 * A gremlin traversal is created from a {@code Query} by appending together fragments in order of priority, one from
 * each {@code MultiTraversal} describing the {@code Query}.
 */
public class Fragment implements Comparable<Fragment> {

    private final UnaryOperator<GraphTraversal<Vertex, Vertex>> traversalFunction;
    private final FragmentPriority priority;
    private final String start;
    private final Optional<String> end;
    private MultiTraversal multiTraversal;

    /**
     * @param traversal a function that extends an existing traversal
     * @param priority the priority of this fragment, describing how expensive the traversal is expected to be
     * @param start the variable name that this fragment starts from in the query
     */
    public Fragment(UnaryOperator<GraphTraversal<Vertex, Vertex>> traversal, FragmentPriority priority, String start) {
        this.traversalFunction = traversal;
        this.priority = priority;
        this.start = start;
        this.end = Optional.empty();
    }

    /**
     * @param traversal a function that extends an existing traversal
     * @param priority the priority of this fragment, describing how expensive the traversal is expected to be
     * @param start the variable name that this fragment starts from in the query
     * @param end the variable name that this fragment ends at in the query
     */
    public Fragment(
            UnaryOperator<GraphTraversal<Vertex, Vertex>> traversal,
            FragmentPriority priority, String start, String end
    ) {
        this.traversalFunction = traversal;
        this.priority = priority;
        this.start = start;
        this.end = Optional.of(end);
    }

    /**
     * @return the MultiTraversal that contains this Fragment
     */
    public MultiTraversal getMultiTraversal() {
        return multiTraversal;
    }

    /**
     * @param multiTraversal the MultiTraversal that contains this Fragment
     */
    void setMultiTraversal(MultiTraversal multiTraversal) {
        this.multiTraversal = multiTraversal;
    }

    /**
     * @param traversal the traversal to extend with this Fragment
     */
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        traversalFunction.apply(traversal);
    }

    /**
     * @return the variable name that this fragment starts from in the query
     */
    public String getStart() {
        return start;
    }

    /**
     * @return the variable name that this fragment ends at in the query, if this query has an end variable
     */
    public Optional<String> getEnd() {
        return end;
    }

    @Override
    /**
     * Order Fragment by priority
     */
    // Don't want to use Jetbrain's @NotNull annotation
    public int compareTo(@SuppressWarnings("NullableProblems") Fragment other) {
        if (this == other) return 0;
        return priority.compareTo(other.priority);
    }
}
