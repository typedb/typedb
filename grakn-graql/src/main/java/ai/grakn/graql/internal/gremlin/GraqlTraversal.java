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
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.util.CommonUtil.toImmutableSet;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

/**
 * A traversal over a Grakn graph, representing one of many ways to execute a {@code MatchQuery}.
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

    // TODO: Find a better way to represent these values
    // Just a pretend big number
    private static final long NUM_VERTICES_ESTIMATE = 10_000;

    private static final long MAX_TRAVERSAL_ATTEMPTS = 1_000;

    private GraqlTraversal(Set<? extends List<Fragment>> fragments) {
        this.fragments = fragments.stream().map(ImmutableList::copyOf).collect(toImmutableSet());
    }

    public static GraqlTraversal create(Set<? extends List<Fragment>> fragments) {
        return new GraqlTraversal(fragments);
    }

    /**
     * Create a semi-optimal traversal plan using a greedy approach
     * @param pattern a pattern to find a query plan for
     * @return a semi-optimal traversal plan
     */
    public static GraqlTraversal semiOptimal(PatternAdmin pattern) {

        Collection<Conjunction<VarAdmin>> patterns = pattern.getDisjunctiveNormalForm().getPatterns();

        // Find a semi-optimal way to execute each conjunction
        Set<? extends List<Fragment>> fragments = patterns.stream()
                .map(ConjunctionQuery::new)
                .map(GraqlTraversal::semiOptimalConjunction)
                .collect(toImmutableSet());

        return GraqlTraversal.create(fragments);
    }

    /**
     * Create a semi-optimal plan using a greedy approach to execute a single conjunction
     * @param query the conjunction query to find a traversal plan
     * @return a semi-optimal traversal plan to execute the given conjunction
     */
    private static List<Fragment> semiOptimalConjunction(ConjunctionQuery query) {

        Set<EquivalentFragmentSet> fragmentSets = Sets.newHashSet(query.getEquivalentFragmentSets());
        Set<VarName> names = new HashSet<>();

        // This list is constructed over the course of the algorithm
        List<Fragment> fragments = new ArrayList<>();

        long numFragments = fragments(fragmentSets).count();
        long depth = 1;
        long numTraversalAttempts = numFragments;

        // Calculate the depth to descend in the tree, based on how many plans we want to evaluate
        while (numFragments > 0 && numTraversalAttempts < MAX_TRAVERSAL_ATTEMPTS) {
            depth += 1;
            numTraversalAttempts *= numFragments;
            numFragments -= 1;
        }

        double cost = 1;

        while (!fragmentSets.isEmpty()) {
            Pair<Double, List<Fragment>> pair = findPlan(fragmentSets, names, cost, depth);
            cost = pair.getValue0();
            List<Fragment> newFragments = Lists.reverse(pair.getValue1());

            if (newFragments.isEmpty()) {
                throw new RuntimeException(ErrorMessage.FAILED_TO_BUILD_TRAVERSAL.getMessage());
            }

            newFragments.forEach(fragment -> {
                fragmentSets.remove(fragment.getEquivalentFragmentSet());
                fragment.getVariableNames().forEach(names::add);
            });
            fragments.addAll(newFragments);
        }

        return fragments;
    }

    /**
     * Find a traversal plan that will satisfy the given equivalent fragment sets
     * @param fragmentSets a set of equivalent fragment sets describing part of a query
     * @param names a set of names that have already been encountered while executing the query
     * @param cost the cost of the query plan so far
     * @param depth the maximum depth the plan is allowed to descend in the tree
     * @return a pair, containing the cost of the plan and a list of fragments comprising the traversal plan
     */
    private static Pair<Double, List<Fragment>> findPlan(
            Set<EquivalentFragmentSet> fragmentSets, Set<VarName> names, double cost, long depth
    ) {
        // Base case
        Pair<Double, List<Fragment>> baseCase = Pair.with(cost, Lists.newArrayList());

        if (depth == 0) return baseCase;

        Comparator<Pair<Double, List<Fragment>>> byCost = comparing(Pair::getValue0);

        // Try every fragment that has its dependencies met, then select the lowest cost fragment
        return fragments(fragmentSets)
                .filter(fragment -> names.containsAll(fragment.getDependencies()))
                .map(fragment -> findPlanWithFragment(fragment, fragmentSets, names, cost, depth))
                .min(byCost)
                .orElse(baseCase);
    }

    private static Pair<Double, List<Fragment>> findPlanWithFragment(
            Fragment fragment, Set<EquivalentFragmentSet> fragmentSets, Set<VarName> names, double cost, long depth
    ) {
        // Calculate the new costs, fragment sets and variable names when using this fragment
        double newCost = fragmentCost(fragment, cost, names);

        EquivalentFragmentSet fragmentSet = fragment.getEquivalentFragmentSet();
        Set<EquivalentFragmentSet> newFragmentSets = Sets.difference(fragmentSets, ImmutableSet.of(fragmentSet));

        Set<VarName> newNames = Sets.union(names, fragment.getVariableNames().collect(toSet()));

        // Recursively find a plan
        Pair<Double, List<Fragment>> pair = findPlan(newFragmentSets, newNames, newCost, depth - 1);

        // Add this fragment and cost and return
        pair.getValue1().add(fragment);
        return pair.setAt0(pair.getValue0() + newCost);
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
            Set<VarName> names = new HashSet<>();

            double cost = 1;
            double listCost = 0;

            for (Fragment fragment : list) {
                cost = fragmentCost(fragment, cost, names);
                fragment.getVariableNames().forEach(names::add);
                listCost += cost;
            }

            totalCost += listCost;
        }

        return totalCost;
    }

    private static double fragmentCost(Fragment fragment, double previousCost, Set<VarName> names) {
        if (names.contains(fragment.getStart())) {
            return fragment.fragmentCost(previousCost);
        } else {
            // Restart traversal, meaning we are navigating from all vertices
            // The constant '1' cost is to discourage constant restarting, even when indexed
            return fragment.fragmentCost(NUM_VERTICES_ESTIMATE) * previousCost + 1;
        }
    }

    private static Stream<Fragment> fragments(Set<EquivalentFragmentSet> fragmentSets) {
        return fragmentSets.stream().flatMap(EquivalentFragmentSet::getFragments);
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
