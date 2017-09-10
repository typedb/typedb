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

import ai.grakn.GraknTx;
import ai.grakn.concept.ConceptId;
import ai.grakn.graql.Match;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.util.Schema;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.stream.Collectors;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static ai.grakn.util.CommonUtil.toImmutableSet;
import static java.util.stream.Collectors.joining;

/**
 * A traversal over a Grakn knowledge base, representing one of many ways to execute a {@link Match}.
 * Comprised of ordered {@code Fragment}s which are used to construct a TinkerPop {@code GraphTraversal}, which can be
 * retrieved and executed.
 *
 * @author Felix Chapman
 */
@AutoValue
public abstract class GraqlTraversal {

    // Just a pretend big number
    private static final long NUM_VERTICES_ESTIMATE = 10_000;
    private static final double COST_NEW_TRAVERSAL = Math.log1p(NUM_VERTICES_ESTIMATE);

    static GraqlTraversal create(Set<? extends List<Fragment>> fragments) {
        ImmutableSet<ImmutableList<Fragment>> copy = fragments.stream().map(ImmutableList::copyOf).collect(toImmutableSet());
        return new AutoValue_GraqlTraversal(copy);
    }

    /**
     * Get the {@code GraphTraversal} that this {@code GraqlTraversal} represents.
     */
    // Because 'union' accepts an array, we can't use generics
    @SuppressWarnings("unchecked")
    public GraphTraversal<Vertex, Map<String, Element>> getGraphTraversal(GraknTx graph) {
        Traversal[] traversals =
                fragments().stream().map(list -> getConjunctionTraversal(graph, list)).toArray(Traversal[]::new);

        return graph.admin().getTinkerTraversal().V().limit(1).union(traversals);
    }

    //       Set of disjunctions
    //        |
    //        |           List of fragments in order of execution
    //        |            |
    //        V            V
    public abstract ImmutableSet<ImmutableList<Fragment>> fragments();


    /**
     *
     * @return
     */
    public GraqlTraversal transform(Map<ConceptId, ConceptId> conceptMap){
        ImmutableList<Fragment> fragments = ImmutableList.copyOf(
                fragments().iterator().next().stream().map(f -> f.transform(conceptMap)).collect(Collectors.toList())
        );
        ImmutableSet<ImmutableList<Fragment>> newFragments = ImmutableSet.of(fragments);
        return new AutoValue_GraqlTraversal(newFragments);
    }

    /**
     * @return a gremlin traversal that represents this inner query
     */
    private GraphTraversal<? extends Element, Map<String, Element>> getConjunctionTraversal(
            GraknTx graph, ImmutableList<Fragment> fragmentList
    ) {
        GraphTraversal traversal = __.V();

        // If the first fragment can operate on edges, then we have to navigate all edges as well
        if (fragmentList.get(0).canOperateOnEdges()) {
            traversal = __.union(traversal, __.V().outE(Schema.EdgeLabel.RESOURCE.getLabel()));
        }

        return applyFragments(graph, fragmentList, traversal);
    }

    private GraphTraversal<?, Map<String, Element>> applyFragments(
            GraknTx graph, ImmutableList<Fragment> fragmentList, GraphTraversal<Element, Element> traversal) {
        Set<Var> foundNames = new HashSet<>();

        // Apply fragments in order into one single traversal
        Var currentName = null;

        for (Fragment fragment : fragmentList) {
            applyFragment(fragment, traversal, currentName, foundNames, graph);
            currentName = fragment.end() != null ? fragment.end() : fragment.start();
        }

        // Select all the variable names
        String[] traversalNames = foundNames.stream().map(Var::getValue).toArray(String[]::new);
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
            Fragment fragment, GraphTraversal<Element, ? extends Element> traversal,
            @Nullable Var currentName, Set<Var> names, GraknTx graph
    ) {
        Var start = fragment.start();

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
        fragment.applyTraversal(traversal, graph);

        Var end = fragment.end();
        if (end != null) {
            if (!names.contains(end)) {
                // This variable name has not been encountered before, remember it and use the 'as' step
                traversal.as(end.getValue());
            } else {
                // This variable name has been encountered before, confirm it is the same
                traversal.where(P.eq(end.getValue()));
            }
        }

        names.addAll(fragment.vars());
    }

    /**
     * Get the estimated complexity of the traversal.
     */
    public double getComplexity() {

        double totalCost = 0;

        for (List<Fragment> list : fragments()) {
            totalCost += fragmentListCost(list);
        }

        return totalCost;
    }

    static double fragmentListCost(List<Fragment> fragments) {
        Set<Var> names = new HashSet<>();

        double cost = 0;
        double listCost = 0;

        for (Fragment fragment : fragments) {
            cost = fragmentCost(fragment, names);
            names.addAll(fragment.vars());
            listCost += cost;
        }

        return listCost;
    }

    static double fragmentCost(Fragment fragment, Collection<Var> names) {
        if (names.contains(fragment.start())) {
            return fragment.fragmentCost();
        } else {
            // Restart traversal, meaning we are navigating from all vertices
            return COST_NEW_TRAVERSAL;
        }
    }

    @Override
    public String toString() {
        return "{" + fragments().stream().map(list -> {
            StringBuilder sb = new StringBuilder();
            Var currentName = null;

            for (Fragment fragment : list) {
                if (!fragment.start().equals(currentName)) {
                    if (currentName != null) sb.append(" ");

                    sb.append(fragment.start().shortName());
                    currentName = fragment.start();
                }

                sb.append(fragment.name());

                Var end = fragment.end();
                if (end != null) {
                    sb.append(end.shortName());
                    currentName = end;
                }
            }

            return sb.toString();
        }).collect(joining(", ")) + "}";
    }
}
