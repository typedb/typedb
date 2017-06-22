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
 *
 */

package ai.grakn.graql.internal.gremlin;

import ai.grakn.GraknGraph;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;
import ai.grakn.graql.internal.gremlin.spanningtree.Arborescence;
import ai.grakn.graql.internal.gremlin.spanningtree.ChuLiuEdmonds;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.DirectedEdge;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.Node;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.SparseWeightedGraph;
import ai.grakn.graql.internal.gremlin.spanningtree.util.Weighted;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.util.CommonUtil.toImmutableSet;

/**
 * Class for generating greedy traversal plans
 *
 * @author Felix Chapman
 */
public class GreedyTraversalPlan {

    private static final long MAX_TRAVERSAL_ATTEMPTS = 15_000;

    // The degree to prune plans - 0.0 means never prune plans, 1.0 means always prune everything except the fastest
    // estimated plan (this is equivalent to a naive greedy algorithm that does not look ahead).
    private static final double PRUNE_FACTOR = 0.1;

    /**
     * Create a traversal plan using the default maxTraersalAttempts.
     *
     * @param pattern a pattern to find a query plan for
     * @return a semi-optimal traversal plan
     * @see GreedyTraversalPlan#createTraversal(PatternAdmin, GraknGraph, long)
     */
    public static GraqlTraversal createTraversal(PatternAdmin pattern, GraknGraph graph) {
        return createTraversal(pattern, graph, MAX_TRAVERSAL_ATTEMPTS);
    }

    /**
     * Create a semi-optimal traversal plan using a greedy approach
     * <p>
     * We can interpret building a plan as a decision tree, where each branch represents selecting a fragment to
     * traverse next. A path from the root to a leaf of the tree represents a traversal plan.
     * <p>
     * Under this definition, this traversal optimisation plan takes a brute-force approach. It traverses a certain
     * number of branches (defined in MAX_TRAVERSAL_ATTEMPTS) and calculates the estimated complexity of each partial
     * plan produced. The lowest complexity partial plan is chosen, then the brute-force search is re-started from that
     * position until a full plan is produced.
     * <p>
     * With fixed MAX_TRAVERSAL_ATTEMPTS, this method is O(n) where n is the size of the query. In general, it produces
     * optimal or nearly-optimal results, so a 'smarter' method may not be necessary.
     *
     * @param pattern              a pattern to find a query plan for
     * @param maxTraversalAttempts number of traversal plans to test
     * @return a semi-optimal traversal plan
     */
    public static GraqlTraversal createTraversal(
            PatternAdmin pattern, GraknGraph graph, long maxTraversalAttempts) {
        Collection<Conjunction<VarPatternAdmin>> patterns = pattern.getDisjunctiveNormalForm().getPatterns();

        // Find a semi-optimal way to execute each conjunction
        Set<? extends List<Fragment>> fragments = patterns.stream()
                .map(conjunction -> new ConjunctionQuery(conjunction, graph))
                .map(query -> semiOptimalConjunction(query, maxTraversalAttempts))
                .collect(toImmutableSet());

        return GraqlTraversal.create(fragments);
    }

    /**
     * Create a semi-optimal plan using a greedy approach to execute a single conjunction
     *
     * @param query the conjunction query to find a traversal plan
     * @return a semi-optimal traversal plan to execute the given conjunction
     */
    private static List<Fragment> semiOptimalConjunction(ConjunctionQuery query, long maxTraversalAttempts) {
        Plan initialPlan = Plan.base();

        Collection<Set<Fragment>> connectedFragmentSets = getConnectedFragmentSets(query);
        System.out.println("connectedFragmentSets = " + connectedFragmentSets.size());
        connectedFragmentSets.forEach(set -> System.out.println("     SetEntry : " + set));

        connectedFragmentSets.forEach(fragmentSet -> {
            final Set<Node> nodesWithFixedCost = new HashSet<>();
            Plan plan = Plan.base();
            Set<Weighted<DirectedEdge<Node>>> weightedGraph = fragmentSet.stream()
                    .filter(fragment -> {
                        if (fragment.hasFixedFragmentCost() && plan.tryPush(fragment)) {
                            nodesWithFixedCost.add(new Node(fragment.getStart()));
                            return false;
                        }
                        return true;
                    })
                    .flatMap(fragment -> fragment.getDirectedEdges().stream())
                    .collect(Collectors.toSet());
            SparseWeightedGraph<Node> sparseWeightedGraph = SparseWeightedGraph.from(weightedGraph);

            final Collection<Node> startingNodes = nodesWithFixedCost.isEmpty() ?
                    sparseWeightedGraph.getNodes() : nodesWithFixedCost;

            Arborescence<Node> arborescence = startingNodes.stream()
                    .map(node -> ChuLiuEdmonds.getMaxArborescence(sparseWeightedGraph, node))
                    .max(Weighted::compareTo).get().val;
            System.out.println("arborescence = " + arborescence);
        });

        // Should always start with fragments with fixed cost
        // So remove them from fragmentSets and add them to the plan
        Set<EquivalentFragmentSet> fragmentSets = query.getEquivalentFragmentSets().stream().filter(fragmentSet -> {
            if (fragmentSet.fragments().size() == 1) {
                Fragment fragment = fragmentSet.fragments().iterator().next();
                if (fragment.hasFixedFragmentCost()) {
                    return !initialPlan.tryPush(fragment);
                }
            }
            return true;
        }).collect(Collectors.toSet());

        long numFragments = fragments(fragmentSets).count();
        long depth = 0;
        long numTraversalAttempts = numFragments;

        // Calculate the depth to descend in the tree, based on how many plans we want to evaluate
        while (numFragments > 0 && numTraversalAttempts < maxTraversalAttempts) {
            depth += 1;
            numTraversalAttempts *= numFragments;
            numFragments -= 1;
        }

        Plan plan = initialPlan.copy();

        while (!fragmentSets.isEmpty()) {
            List<Plan> allPlans = Lists.newArrayList();
            extendPlan(plan, allPlans, fragmentSets, depth);

            Plan newPlan = Collections.min(allPlans);

            // Only retain one new fragment
            // TODO: Find a more elegant way to do this?
            while (newPlan.size() > plan.size() + 1) {
                newPlan.pop();
            }

            plan = newPlan;

            plan.fragments().forEach(fragment -> fragmentSets.remove(fragment.getEquivalentFragmentSet()));
        }
        System.out.println("plan.fragments() = " + plan.fragments());
        System.out.println();

        return plan.fragments();
    }

    private static Collection<Set<Fragment>> getConnectedFragmentSets(ConjunctionQuery query) {
        Map<Integer, Set<String>> varNameSetMap = new HashMap<>();
        Map<Integer, Set<Fragment>> fragmentSetMap = new HashMap<>();
        final int[] index = {0};
        query.getEquivalentFragmentSets().stream().flatMap(EquivalentFragmentSet::stream).forEach(fragment -> {

            Set<Var> fragmentVarsSet = Sets.newHashSet(fragment.getVariableNames());
            fragmentVarsSet.addAll(fragment.getDependencies());
            Set<String> fragmentVarNameSet = fragmentVarsSet.stream().map(Var::getValue).collect(Collectors.toSet());

            List<Integer> setsWithVarInCommon = new ArrayList<>();
            varNameSetMap.forEach((setIndex, varNameSet) -> {
                if (!Collections.disjoint(varNameSet, fragmentVarNameSet)) {
                    setsWithVarInCommon.add(setIndex);
                }
            });

            if (setsWithVarInCommon.isEmpty()) {
                index[0] += 1;
                varNameSetMap.put(index[0], fragmentVarNameSet);
                fragmentSetMap.put(index[0], Sets.newHashSet(fragment));
            } else {
                Iterator<Integer> iterator = setsWithVarInCommon.iterator();
                Integer firstSet = iterator.next();
                varNameSetMap.get(firstSet).addAll(fragmentVarNameSet);
                fragmentSetMap.get(firstSet).add(fragment);
                while (iterator.hasNext()) {
                    Integer nextSet = iterator.next();
                    varNameSetMap.get(firstSet).addAll(varNameSetMap.get(nextSet));
                    fragmentSetMap.get(firstSet).addAll(fragmentSetMap.get(nextSet));
                    varNameSetMap.remove(nextSet);
                    fragmentSetMap.remove(nextSet);
                }
            }
        });
        return fragmentSetMap.values();
    }

    /**
     * Find a traversal plan that will satisfy the given equivalent fragment sets
     *
     * @param plan         the plan so far
     * @param fragmentSets a set of equivalent fragment sets that must all be covered by the plan
     * @param depth        the maximum depth the plan is allowed to descend in the tree
     */
    private static void extendPlan(Plan plan, List<Plan> allPlans,
                                   Set<EquivalentFragmentSet> fragmentSets, long depth) {

        // Base case
        if (depth == 0) {
            allPlans.add(plan.copy());
            return;
        }

        // The minimum cost of all plan with only one additional fragment. Used for deciding which branches to prune.
        double minPartialPlanCost = Double.MAX_VALUE;

        for (EquivalentFragmentSet fragmentSet : fragmentSets) {
            for (Fragment fragment : fragmentSet.fragments()) {

                if (plan.tryPush(fragment)) {
                    minPartialPlanCost = Math.min(plan.cost(), minPartialPlanCost);
                    plan.pop();
                }
            }
        }

        boolean addedPlan = false;

        for (EquivalentFragmentSet fragmentSet : fragmentSets) {
            for (Fragment fragment : fragmentSet.fragments()) {

                // Skip fragments that don't have their dependencies met or are in sets that have already been visited
                if (!plan.tryPush(fragment)) {
                    continue;
                }

                // Prune any plans that are much more expensive than the cheapest partial plan
                if (plan.cost() * PRUNE_FACTOR <= minPartialPlanCost) {
                    addedPlan = true;

                    // Recursively find a plan
                    extendPlan(plan, allPlans, fragmentSets, depth - 1);
                }

                plan.pop();
            }
        }

        if (!addedPlan) {
            allPlans.add(plan.copy());
        }
    }

    private static Stream<Fragment> fragments(Set<EquivalentFragmentSet> fragmentSets) {
        return fragmentSets.stream().flatMap(EquivalentFragmentSet::stream);
    }
}
