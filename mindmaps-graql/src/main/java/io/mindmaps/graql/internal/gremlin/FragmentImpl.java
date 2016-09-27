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
 *
 */

package io.mindmaps.graql.internal.gremlin;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Optional;
import java.util.function.UnaryOperator;

public class FragmentImpl implements Fragment {

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
    public FragmentImpl(UnaryOperator<GraphTraversal<Vertex, Vertex>> traversal, FragmentPriority priority, String start) {
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
    FragmentImpl(
            UnaryOperator<GraphTraversal<Vertex, Vertex>> traversal,
            FragmentPriority priority, String start, String end
    ) {
        this.traversalFunction = traversal;
        this.priority = priority;
        this.start = start;
        this.end = Optional.of(end);
    }

    @Override
    public MultiTraversal getMultiTraversal() {
        return multiTraversal;
    }

    @Override
    public void setMultiTraversal(MultiTraversal multiTraversal) {
        this.multiTraversal = multiTraversal;
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        traversalFunction.apply(traversal);
    }

    @Override
    public String getStart() {
        return start;
    }

    @Override
    public Optional<String> getEnd() {
        return end;
    }

    @Override
    public FragmentPriority getPriority() {
        return priority;
    }

    /**
     * Order Fragment by priority
     */
    @Override
    public int compareTo(@SuppressWarnings("NullableProblems") Fragment other) {
        // Don't want to use Jetbrain's @NotNull annotation
        if (this == other) return 0;
        return priority.compareTo(other.getPriority());
    }
}
