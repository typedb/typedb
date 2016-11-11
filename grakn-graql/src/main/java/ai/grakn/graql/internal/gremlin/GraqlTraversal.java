/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs
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

package ai.grakn.graql.internal.gremlin;

import ai.grakn.MindmapsGraph;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;
import ai.grakn.graql.internal.util.CommonUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import ai.grakn.MindmapsGraph;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static ai.grakn.graql.internal.util.CommonUtil.toImmutableSet;
import static java.util.stream.Collectors.joining;

/**
 * A traversal over a Mindmaps graph, representing one of many ways to execute a {@code MatchQuery}.
 * Comprised of ordered {@code Fragment}s which are used to construct a TinkerPop {@code GraphTraversal}, which can be
 * retrieved and executed.
 */
public class GraqlTraversal {

    //            Set of disjunctions
    //             |
    //             |           List of fragments in order of execution
    //             |            |
    //             V            V
    private final ImmutableSet<ImmutableList<Fragment>> fragments;
    private final MindmapsGraph graph;

    private GraqlTraversal(MindmapsGraph graph, Set<? extends List<Fragment>> fragments) {
        this.graph = graph;
        this.fragments = fragments.stream().map(ImmutableList::copyOf).collect(CommonUtil.toImmutableSet());
    }

    public static GraqlTraversal create(MindmapsGraph graph, Set<? extends List<Fragment>> fragments) {
        return new GraqlTraversal(graph, fragments);
    }

    /**
     * Get the {@code GraphTraversal} that this {@code GraqlTraversal} represents.
     */
    GraphTraversal<Vertex, Map<String, Vertex>> getGraphTraversal() {
        Traversal[] traversals =
                fragments.stream().map(this::getConjunctionTraversal).toArray(Traversal[]::new);

        // Because 'union' accepts an array, we can't use generics...
        //noinspection unchecked
        return graph.getTinkerTraversal().limit(1).union(traversals);
    }

    /**
     * @return a gremlin traversal that represents this inner query
     */
    private GraphTraversal<Vertex, Map<String, Vertex>> getConjunctionTraversal(ImmutableList<Fragment> fragmentList) {
        GraphTraversal<Vertex, Vertex> traversal = graph.getTinkerTraversal();

        Set<String> foundNames = new HashSet<>();

        // Apply fragments in order into one single traversal
        String currentName = null;

        for (Fragment fragment : fragmentList) {
            applyFragment(fragment, traversal, currentName, foundNames);
            currentName = fragment.getEnd().orElse(fragment.getStart());
        }

        // Select all the variable names
        String[] traversalNames = foundNames.toArray(new String[foundNames.size()]);
        return traversal.select(traversalNames[0], traversalNames[0], traversalNames);
    }

    /**
     * Apply the given fragment to the traversal. Keeps track of variable names so far so that it can decide whether
     * to use "as" or "select" steps in gremlin.
     * @param fragment the fragment to apply to the traversal
     * @param traversal the gremlin traversal to apply the fragment to
     * @param currentName the variable name that the traversal is currently at
     * @param names a set of variable names so far encountered in the query
     */
    private void applyFragment(
            Fragment fragment, GraphTraversal<Vertex, Vertex> traversal, String currentName, Set<String> names
    ) {
        String start = fragment.getStart();

        if (currentName != null) {
            if (!currentName.equals(start)) {
                if (names.contains(start)) {
                    // If the variable name has been visited but the traversal is not at that variable name, select it
                    traversal.select(start);
                } else {
                    // Restart traversal when fragments are disconnected
                    traversal.V().as(start);
                }
            }
        } else {
            // If the variable name has not been visited yet, remember it and use the 'as' step
            traversal.as(start);
        }

        names.add(start);

        // Apply fragment to traversal
        fragment.applyTraversal(traversal);

        fragment.getEnd().ifPresent(end -> {
            if (!names.contains(end)) {
                // This variable name has not been encountered before, remember it and use the 'as' step
                names.add(end);
                traversal.as(end);
            } else {
                // This variable name has been encountered before, confirm it is the same
                traversal.where(P.eq(end));
            }
        });
    }

    /**
     * Get the estimated complexity of the traversal.
     */
    public long getComplexity() {

        // TODO: Find a better way to represent these values
        // Just a pretend big number
        long NUM_VERTICES_ESTIMATE = 1_000;

        Set<String> names = new HashSet<>();

        long totalCost = 0;

        for (List<Fragment> list : fragments) {
            long currentCost;
            long previousCost = 1;
            long listCost = 0;

            for (Fragment fragment : list) {
                String start = fragment.getStart();

                if (names.contains(start)) {
                    currentCost = fragment.fragmentCost(previousCost);
                } else {
                    // Restart traversal, meaning we are navigating from all vertices
                    // The constant '1' cost is to discourage constant restarting, even when indexed
                    currentCost = fragment.fragmentCost(NUM_VERTICES_ESTIMATE) * previousCost + 1;
                }

                names.add(start);
                fragment.getEnd().ifPresent(names::add);

                listCost += currentCost;
                previousCost = currentCost;
            }

            totalCost += listCost;
        }

        return totalCost;
    }

    @Override
    public String toString() {
        return "{" + fragments.stream().map(list -> {
            StringBuilder sb = new StringBuilder();
            String currentName = null;

            for (Fragment fragment : list) {
                if (!fragment.getStart().equals(currentName)) {
                    if (currentName != null) sb.append(" ");

                    sb.append("$").append(StringUtils.left(fragment.getStart(), 3));
                    currentName = fragment.getStart();
                }

                sb.append(fragment.getName());

                Optional<String> end = fragment.getEnd();
                if (end.isPresent()) {
                    sb.append("$").append(StringUtils.left(end.get(), 3));
                    currentName = end.get();
                }
            }

            return sb.toString();
        }).collect(joining(", ")) + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GraqlTraversal that = (GraqlTraversal) o;

        if (fragments != null ? !fragments.equals(that.fragments) : that.fragments != null) return false;
        return graph != null ? graph.equals(that.graph) : that.graph == null;

    }

    @Override
    public int hashCode() {
        int result = fragments != null ? fragments.hashCode() : 0;
        result = 31 * result + (graph != null ? graph.hashCode() : 0);
        return result;
    }
}
