/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.graql.planning;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import grakn.core.core.JanusTraversalSourceProvider;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.planning.gremlin.Fragment;
import grakn.core.kb.graql.planning.gremlin.GraqlTraversal;
import graql.lang.statement.Variable;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectStep;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;

/**
 * A traversal over a Grakn knowledge base, representing one of many ways to execute a match clause
 * Comprised of ordered {@code Fragment}s which are used to construct a TinkerPop {@code GraphTraversal}, which can be
 * retrieved and executed.
 */
public class GraqlTraversalImpl implements GraqlTraversal {

    private final ImmutableSet<ImmutableList<? extends Fragment>> fragments;
    private JanusTraversalSourceProvider janusTraversalSourceProvider;
    private ConceptManager conceptManager;

    // Just a pretend big number
    private static final long NUM_VERTICES_ESTIMATE = 10_000;
    private static final double COST_NEW_TRAVERSAL = Math.log1p(NUM_VERTICES_ESTIMATE);


    GraqlTraversalImpl(JanusTraversalSourceProvider janusTraversalSourceProvider, ConceptManager conceptManager,
                       Set<List<? extends Fragment>> fragments) {
        this.janusTraversalSourceProvider = janusTraversalSourceProvider;
        this.conceptManager = conceptManager;
        // copy the fragments
        this.fragments = fragments.stream().map(ImmutableList::copyOf).collect(ImmutableSet.toImmutableSet());
    }

    @Override
    public ImmutableSet<ImmutableList<? extends Fragment>> fragments() {
        return fragments;
    }

    /**
     * Get the {@code GraphTraversal} that this {@code GraqlTraversal} represents.
     */
    @Override
    // Because 'union' accepts an array, we can't use generics
    @SuppressWarnings("unchecked")
    public GraphTraversal<Vertex, Map<String, Element>> getGraphTraversal(Set<Variable> vars) {

        if (fragments().size() == 1) {
            // If there are no disjunctions, we don't need to union them and get a performance boost
            ImmutableList<? extends Fragment> list = Iterables.getOnlyElement(fragments());
            return getConjunctionTraversal(janusTraversalSourceProvider.getTinkerTraversal().V(), vars, list);
        } else {
            Traversal[] traversals = fragments().stream()
                    .map(list -> getConjunctionTraversal(__.V(), vars, list))
                    .toArray(Traversal[]::new);

            // This is a sneaky trick - we want to do a union but tinkerpop requires all traversals to start from
            // somewhere, so we start from a single arbitrary vertex.
            GraphTraversal traversal = janusTraversalSourceProvider.getTinkerTraversal().V().limit(1).union(traversals);

            return selectVars(traversal, vars);
        }
    }

    /**
     * @param transform map defining id transform var -> new id
     * @return graql traversal with concept id transformed according to the provided transform
     */
    @Override
    public GraqlTraversal transform(Map<Variable, ConceptId> transform) {
        ImmutableList<Fragment> fragments = ImmutableList.copyOf(
                Iterables.getOnlyElement(fragments()).stream().map(f -> f.transform(transform)).collect(Collectors.toList())
        );
        return new GraqlTraversalImpl(janusTraversalSourceProvider, conceptManager, ImmutableSet.of(fragments));
    }

    /**
     * @return a gremlin traversal that represents this inner query
     */
    private GraphTraversal<Vertex, Map<String, Element>> getConjunctionTraversal(
            GraphTraversal<Vertex, Vertex> traversal, Set<Variable> vars,
            ImmutableList<? extends Fragment> fragmentList) {

        return applyFragments(vars, fragmentList, traversal);
    }

    private GraphTraversal<Vertex, Map<String, Element>> applyFragments(
            Set<Variable> vars, ImmutableList<? extends Fragment> fragmentList,
            GraphTraversal<Vertex, ? extends Element> traversal) {
        Set<Variable> foundVars = new HashSet<>();

        // Apply fragments in order into one single traversal
        Variable currentName = null;

        for (Fragment fragment : fragmentList) {
            // Apply fragment to traversal
            fragment.applyTraversal(traversal, conceptManager, foundVars, currentName);
            currentName = fragment.end() != null ? fragment.end() : fragment.start();
        }

        // Select all the variable names
        return selectVars(traversal, Sets.intersection(vars, foundVars));
    }

    /**
     * Get the estimated complexity of the traversal.
     */
    public double getComplexity() {

        double totalCost = 0;

        for (List<? extends Fragment> list : fragments()) {
            totalCost += fragmentListCost(list);
        }

        return totalCost;
    }

    private static double fragmentListCost(List<? extends Fragment> fragments) {
        Set<Variable> names = new HashSet<>();

        double listCost = 0;

        for (Fragment fragment : fragments) {
            listCost += fragmentCost(fragment, names);
            names.addAll(fragment.vars());
        }

        return listCost;
    }

    private static double fragmentCost(Fragment fragment, Collection<Variable> names) {
        if (names.contains(fragment.start()) || fragment.hasFixedFragmentCost()) {
            return fragment.fragmentCost();
        } else {
            // Restart traversal, meaning we are navigating from all vertices
            return COST_NEW_TRAVERSAL;
        }
    }

    private static <S, E> GraphTraversal<S, Map<String, E>> selectVars(GraphTraversal<S, ?> traversal, Set<Variable> vars) {
        if (vars.isEmpty()) {
            // Produce an empty result
            return traversal.constant(ImmutableMap.of());
        } else if (vars.size() == 1) {
            String label = vars.iterator().next().symbol();
            return traversal.select(label, label);
        } else {
            String[] labelArray = vars.stream().map(Variable::symbol).toArray(String[]::new);
            return traversal.asAdmin().addStep(new SelectStep<>(traversal.asAdmin(), null, labelArray));
        }
    }

    @Override
    public String toString() {
        return "{" + fragments().stream().map(list -> {
            StringBuilder sb = new StringBuilder();
            Variable currentName = null;

            for (Fragment fragment : list) {
                if (!fragment.start().equals(currentName)) {
                    if (currentName != null) sb.append(" ");

                    sb.append(fragment.start().symbol());
                    currentName = fragment.start();
                }

                sb.append(fragment.name());

                Variable end = fragment.end();
                if (end != null) {
                    sb.append(end.symbol());
                    currentName = end;
                }
            }

            return sb.toString();
        }).collect(joining(", ")) + "}";
    }
}
