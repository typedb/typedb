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

package ai.grakn.graql.internal.gremlin;

import ai.grakn.GraknGraph;
import ai.grakn.graql.VarName;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static ai.grakn.graql.internal.util.CommonUtil.toImmutableSet;
import static java.util.stream.Collectors.joining;

/**
 * A traversal over a Grakn graph, representing one of many ways to execute a {@code MatchQuery}.
 * Comprised of ordered {@code Fragment}s which are used to construct a TinkerPop {@code GraphTraversal}, which can be
 * retrieved and executed.
 *
 * @author Felix Chapman
 */
public class GraqlTraversal {

    //            Set of disjunctions
    //             |
    //             |           List of fragments in order of execution
    //             |            |
    //             V            V
    private final ImmutableSet<ImmutableList<Fragment>> fragments;

    // TODO: Find a better way to represent these values
    // Just a pretend big number
    private static final long NUM_VERTICES_ESTIMATE = 10_000;

    private GraqlTraversal(Set<? extends List<Fragment>> fragments) {
        this.fragments = fragments.stream().map(ImmutableList::copyOf).collect(toImmutableSet());
    }

    static GraqlTraversal create(Set<? extends List<Fragment>> fragments) {
        return new GraqlTraversal(fragments);
    }

    /**
     * Get the {@code GraphTraversal} that this {@code GraqlTraversal} represents.
     */
    // Because 'union' accepts an array, we can't use generics
    @SuppressWarnings("unchecked")
    public GraphTraversal<Vertex, Map<String, Vertex>> getGraphTraversal(GraknGraph graph) {
        Traversal[] traversals =
                fragments.stream().map(list -> getConjunctionTraversal(graph, list)).toArray(Traversal[]::new);

        return graph.admin().getTinkerTraversal().limit(1).union(traversals);
    }

    /**
     * @return a gremlin traversal that represents this inner query
     */
    private GraphTraversal<Vertex, Map<String, Vertex>> getConjunctionTraversal(
            GraknGraph graph, ImmutableList<Fragment> fragmentList
    ) {
        GraphTraversal<Vertex, Vertex> traversal = graph.admin().getTinkerTraversal();

        Set<VarName> foundNames = new HashSet<>();

        // Apply fragments in order into one single traversal
        VarName currentName = null;

        for (Fragment fragment : fragmentList) {
            applyFragment(fragment, traversal, currentName, foundNames);
            currentName = fragment.getEnd().orElse(fragment.getStart());
        }

        // Select all the variable names
        String[] traversalNames = foundNames.stream().map(VarName::getValue).toArray(String[]::new);
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
            Fragment fragment, GraphTraversal<Vertex, Vertex> traversal, VarName currentName, Set<VarName> names
    ) {
        VarName start = fragment.getStart();

        if (currentName != null) {
            if (!currentName.equals(start)) {
                if (names.contains(start)) {
                    // If the variable name has been visited but the traversal is not at that variable name, select it
                    traversal.select(start.getValue());
                } else {
                    // Restart traversal when fragments are disconnected
                    traversal.V().as(start.getValue());
                }
            }
        } else {
            // If the variable name has not been visited yet, remember it and use the 'as' step
            traversal.as(start.getValue());
        }

        names.add(start);

        // Apply fragment to traversal
        fragment.applyTraversal(traversal);

        fragment.getEnd().ifPresent(end -> {
            if (!names.contains(end)) {
                // This variable name has not been encountered before, remember it and use the 'as' step
                names.add(end);
                traversal.as(end.getValue());
            } else {
                // This variable name has been encountered before, confirm it is the same
                traversal.where(P.eq(end.getValue()));
            }
        });
    }

    /**
     * Get the estimated complexity of the traversal.
     */
    public double getComplexity() {

        double totalCost = 0;

        for (List<Fragment> list : fragments) {
            totalCost += fragmentListCost(list);
        }

        return totalCost;
    }

    static double fragmentListCost(List<Fragment> fragments) {
        Set<VarName> names = new HashSet<>();

        double cost = 1;
        double listCost = 0;

        for (Fragment fragment : fragments) {
            cost = fragmentCost(fragment, cost, names);
            names.addAll(fragment.getVariableNames());
            listCost += cost;
        }

        return listCost;
    }

    static double fragmentCost(Fragment fragment, double previousCost, Collection<VarName> names) {
        if (names.contains(fragment.getStart())) {
            return fragment.fragmentCost(previousCost);
        } else {
            // Restart traversal, meaning we are navigating from all vertices
            return fragment.fragmentCost(NUM_VERTICES_ESTIMATE) * previousCost;
        }
    }

    @Override
    public String toString() {
        return "{" + fragments.stream().map(list -> {
            StringBuilder sb = new StringBuilder();
            VarName currentName = null;

            for (Fragment fragment : list) {
                if (!fragment.getStart().equals(currentName)) {
                    if (currentName != null) sb.append(" ");

                    sb.append(fragment.getStart().shortName());
                    currentName = fragment.getStart();
                }

                sb.append(fragment.getName());

                Optional<VarName> end = fragment.getEnd();
                if (end.isPresent()) {
                    sb.append(end.get().shortName());
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

        return fragments.equals(that.fragments);
    }

    @Override
    public int hashCode() {
        return fragments.hashCode();
    }
}
