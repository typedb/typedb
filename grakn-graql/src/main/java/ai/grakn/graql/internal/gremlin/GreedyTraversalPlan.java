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
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.util.CommonUtil.toImmutableSet;

/**
 * Class for generating greedy traversal plans
 *
 * @author Felix Chapman
 */
public class GreedyTraversalPlan {

    /**
     * Create a traversal plan.
     *
     * @param pattern a pattern to find a query plan for
     * @return a semi-optimal traversal plan
     */
    public static GraqlTraversal createTraversal(PatternAdmin pattern, GraknGraph graph) {
        Collection<Conjunction<VarPatternAdmin>> patterns = pattern.getDisjunctiveNormalForm().getPatterns();

        Set<? extends List<Fragment>> fragments = patterns.stream()
                .map(conjunction -> new ConjunctionQuery(conjunction, graph))
                .map(GreedyTraversalPlan::semiOptimalConjunction)
                .collect(toImmutableSet());

        return GraqlTraversal.create(fragments);
    }

    /**
     * Create a plan using Edmonds' algorithm with greedy approach to execute a single conjunction
     *
     * @param query the conjunction query to find a traversal plan
     * @return a semi-optimal traversal plan to execute the given conjunction
     */
    private static List<Fragment> semiOptimalConjunction(ConjunctionQuery query) {
        List<Fragment> plan = new ArrayList<>();

        Collection<Set<Fragment>> connectedFragmentSets = getConnectedFragmentSets(query);
        System.out.println("connectedFragmentSets = " + connectedFragmentSets.size());
        connectedFragmentSets.forEach(set -> System.out.println("     SetEntry : " + set));

        connectedFragmentSets.forEach(fragmentSet -> {
            Map<String, Node> nodes = new HashMap<>();
            Map<Node, Map<Node, Fragment>> edges = new HashMap<>();
            final Set<Node> nodesWithFixedCost = new HashSet<>();
            Set<Weighted<DirectedEdge<Node>>> weightedGraph = fragmentSet.stream()
                    .filter(fragment -> {

                        if (!fragment.getEnd().isPresent()) {
                            Node node = Node.addIfAbsent(fragment.getStart(), nodes);

                            if (fragment.hasFixedFragmentCost()) {
                                // fragments that should be done right away
                                plan.add(fragment);
                                nodesWithFixedCost.add(node);
                                return false;

                            } else if (fragment.getDependencies().isEmpty()) {
                                //fragments that should be done when a node has been visited
                                node.getFragmentsWithoutDependency().add(fragment);
                                return false;

                            } else {
                                // check if the fragment is neq
                                if (fragment.getEquivalentFragmentSet().fragments().size() == 2) {
                                    node.getFragmentsWithDependency().add(fragment);
                                    fragment.getDependencies().forEach(var -> {
                                        Node dependency = Node.addIfAbsent(var, nodes);
                                        dependency.getDependants().add(fragment);
                                    });
                                    return false;
                                }
                                // else it's value fragment with dependencies
                                return true;
                            }
                        }
                        return true;
                    })
                    .flatMap(fragment -> fragment.getDirectedEdges(nodes, edges).stream())
                    .collect(Collectors.toSet());

            // if there is no edge fragment
            if (!weightedGraph.isEmpty()) {
                SparseWeightedGraph<Node> sparseWeightedGraph = SparseWeightedGraph.from(weightedGraph);

                final Collection<Node> startingNodes = nodesWithFixedCost.isEmpty() ?
                        sparseWeightedGraph.getNodes() : nodesWithFixedCost;

                Arborescence<Node> arborescence = startingNodes.stream()
                        .map(node -> ChuLiuEdmonds.getMaxArborescence(sparseWeightedGraph, node))
                        .max(Weighted::compareTo)
                        .map(arborescenceInside -> arborescenceInside.val).orElse(Arborescence.empty());

                System.out.println("arborescence.getRoot() = " + arborescence.getRoot());
                System.out.println("arborescence = " + arborescence);
                greedyTraversal(plan, arborescence, nodes, edges);
            }

            // add the remaining node fragments
            Set<Node> nodeWithFragment = nodes.values().stream()
                    .filter(node -> !node.getFragmentsWithoutDependency().isEmpty())
                    .collect(Collectors.toSet());
            while (!nodeWithFragment.isEmpty()) {
                nodeWithFragment.forEach(node -> addNodeFragmentToPlan(node, plan, nodes));
                nodeWithFragment = nodes.values().stream()
                        .filter(node -> !node.getFragmentsWithoutDependency().isEmpty())
                        .collect(Collectors.toSet());
            }
        });
        System.out.println();
        System.out.println("final plan = " + plan);
        System.out.println();

        return plan;
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

    private static void greedyTraversal(List<Fragment> plan, Arborescence<Node> arborescence,
                                        Map<String, Node> nodes,
                                        Map<Node, Map<Node, Fragment>> edgeFragmentChildToParent) {
        Map<Node, Set<Node>> edgesParentToChild = new HashMap<>();
        arborescence.getParents().forEach((child, parent) -> {
            if (!edgesParentToChild.containsKey(parent)) {
                edgesParentToChild.put(parent, new HashSet<>());
            }
            edgesParentToChild.get(parent).add(child);
        });

        Node root = arborescence.getRoot();
        Set<Node> reachableNodes = Sets.newHashSet(root);
        while (!reachableNodes.isEmpty()) {
            System.out.println("reachableNodes = " + reachableNodes);
            Node nodeWithMinCost = reachableNodes.stream().min(Comparator.comparingDouble(node ->
                    getEdgeFragmentCost(node, arborescence, edgeFragmentChildToParent))).get();

            // add edge fragment first, then node fragments
            getEdgeFragment(nodeWithMinCost, arborescence, edgeFragmentChildToParent).ifPresent(plan::add);
            addNodeFragmentToPlan(nodeWithMinCost, plan, nodes);

            reachableNodes.remove(nodeWithMinCost);
            if (edgesParentToChild.containsKey(nodeWithMinCost)) {
                reachableNodes.addAll(edgesParentToChild.get(nodeWithMinCost));
            }

            System.out.println("plan = " + plan);
        }
    }

    private static void addNodeFragmentToPlan(Node node, List<Fragment> plan, Map<String, Node> nodes) {
        plan.addAll(node.getFragmentsWithoutDependency().stream()
                .sorted(Comparator.comparingDouble(fragment -> fragment.fragmentCost(0)))
                .collect(Collectors.toList()));
        node.getFragmentsWithoutDependency().clear();

        if (!node.getFragmentsWithDependency().isEmpty()) {
            node.getDependants().forEach(fragment -> {
                Node otherNode = Node.addIfAbsent(fragment.getStart(), nodes);
                otherNode.getFragmentsWithoutDependency().add(fragment);
                otherNode.getFragmentsWithDependency().remove(fragment);
            });
            node.getFragmentsWithDependency().clear();
        }

        node.getDependants().clear();
    }

    private static double getEdgeFragmentCost(Node node, Arborescence<Node> arborescence,
                                              Map<Node, Map<Node, Fragment>> edgeToFragment) {
        Optional<Fragment> fragment = getEdgeFragment(node, arborescence, edgeToFragment);
        return fragment.map(fragmentInside -> fragmentInside.fragmentCost(0)).orElse(0D);
    }

    private static Optional<Fragment> getEdgeFragment(Node node, Arborescence<Node> arborescence,
                                                      Map<Node, Map<Node, Fragment>> edgeToFragment) {
        if (edgeToFragment.containsKey(node) &&
                edgeToFragment.get(node).containsKey(arborescence.getParents().get(node))) {
            return Optional.of(edgeToFragment.get(node).get(arborescence.getParents().get(node)));
        }
        return Optional.empty();
    }
}
