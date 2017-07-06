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
 * @author Jason Liu
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
                .map(GreedyTraversalPlan::planForConjunction)
                .collect(toImmutableSet());

        return GraqlTraversal.create(fragments);
    }

    /**
     * Create a plan using Edmonds' algorithm with greedy approach to execute a single conjunction
     *
     * @param query the conjunction query to find a traversal plan
     * @return a semi-optimal traversal plan to execute the given conjunction
     */
    private static List<Fragment> planForConjunction(ConjunctionQuery query) {

        List<Fragment> plan = new ArrayList<>();
        Map<String, Node> allNodes = new HashMap<>();

        Collection<Set<Fragment>> connectedFragmentSets = getConnectedFragmentSets(query, allNodes);

        System.out.println("connectedFragmentSets.size() = " + connectedFragmentSets.size());
        System.out.println("connectedFragmentSets = " + connectedFragmentSets);

//        Map<String, Fragment> neqGlobal = new HashMap<>();
        connectedFragmentSets.forEach(fragmentSet -> {

            Set<Node> connectedNodes = new HashSet<>();
            Map<Node, Map<Node, Fragment>> edges = new HashMap<>();

//            Set<Fragment> neqLocal = new HashSet<>();
//            Set<Fragment> valueLocal = new HashSet<>();
            final Set<Node> nodesWithFixedCost = new HashSet<>();

            Set<Weighted<DirectedEdge<Node>>> weightedGraph = new HashSet<>();

            System.out.println("fragmentSet = " + fragmentSet);
            fragmentSet.stream()
                    .filter(fragment -> {
                        if (!fragment.getEnd().isPresent()) {
                            Node start = Node.addIfAbsent(fragment.getStart(), allNodes);
                            connectedNodes.add(start);

                            if (fragment.hasFixedFragmentCost()) {
                                // fragments that should be done right away
                                plan.add(fragment);
                                nodesWithFixedCost.add(start);

                            } else if (fragment.getDependencies().isEmpty()) {
                                //fragments that should be done when a node has been visited
                                start.getFragmentsWithoutDependency().add(fragment);

                            }
                            return false;

                        } else {
                            return true;
                        }
                    })
                    .flatMap(fragment -> fragment.getDirectedEdges(allNodes, edges).stream())
                    .forEach(weightedDirectedEdge -> {
                        weightedGraph.add(weightedDirectedEdge);
                        connectedNodes.add(weightedDirectedEdge.val.destination);
                        connectedNodes.add(weightedDirectedEdge.val.source);
                    });
//                    .collect(Collectors.toSet());

            // if there is no edge fragment
            if (!weightedGraph.isEmpty()) {
                SparseWeightedGraph<Node> sparseWeightedGraph = SparseWeightedGraph.from(weightedGraph);

                final Collection<Node> startingNodes = nodesWithFixedCost.isEmpty() ?
                        sparseWeightedGraph.getNodes().stream()
                                .filter(Node::isValidStartingPoint).collect(Collectors.toSet()) : nodesWithFixedCost;

                Arborescence<Node> arborescence = startingNodes.stream()
                        .map(node -> {
                            Weighted<Arborescence<Node>> maxArborescence =
                                    ChuLiuEdmonds.getMaxArborescence(sparseWeightedGraph, node);
                            System.out.println("node = " + node);
                            System.out.println("maxArborescence = " + maxArborescence.weight);
                            return maxArborescence;
                        })
//                        .max(Weighted::compareTo)
                        .max(Comparator.comparingDouble(tree -> tree.weight))
                        .map(arborescenceInside -> arborescenceInside.val).orElse(Arborescence.empty());
                System.out.println("arborescence.getRoot() = " + arborescence.getRoot());

                greedyTraversal(plan, arborescence, allNodes, edges);
            }

//            System.out.println("plan = " + plan);
            // add the remaining node fragments
//            Set<Node> nodeWithFragment = allNodes.values().stream()
            Set<Node> nodeWithFragment = connectedNodes.stream()
                    .filter(node -> !node.getFragmentsWithoutDependency().isEmpty() ||
                            !node.getFragmentsWithDependencyVisited().isEmpty())
                    .collect(Collectors.toSet());
            while (!nodeWithFragment.isEmpty()) {
//                System.out.println("check");
                nodeWithFragment.forEach(node -> addNodeFragmentToPlan(node, plan, allNodes, false));
                nodeWithFragment = connectedNodes.stream()
                        .filter(node -> !node.getFragmentsWithoutDependency().isEmpty() ||
                                !node.getFragmentsWithDependencyVisited().isEmpty())
                        .collect(Collectors.toSet());
            }
        });
        // apply global neq
//        plan.addAll(neqGlobal.values());

        Set<Node> nodeWithFragment = allNodes.values().stream()
                .filter(node -> !node.getFragmentsWithoutDependency().isEmpty() ||
                        !node.getFragmentsWithDependencyVisited().isEmpty())
                .collect(Collectors.toSet());
        while (!nodeWithFragment.isEmpty()) {
//            System.out.println("final check");

            nodeWithFragment.forEach(node -> addNodeFragmentToPlan(node, plan, allNodes, false));
            nodeWithFragment = allNodes.values().stream()
                    .filter(node -> !node.getFragmentsWithoutDependency().isEmpty() ||
                            !node.getFragmentsWithDependencyVisited().isEmpty())
                    .collect(Collectors.toSet());
        }

        System.out.println("final plan = " + plan);
        return plan;
    }

    private static Collection<Set<Fragment>> getConnectedFragmentSets(ConjunctionQuery query,
                                                                      Map<String, Node> allNodes) {
        Map<Integer, Set<String>> varNameSetMap = new HashMap<>();
        Map<Integer, Set<Fragment>> fragmentSetMap = new HashMap<>();
        final int[] index = {0};
        query.getEquivalentFragmentSets().stream().flatMap(EquivalentFragmentSet::stream).forEach(fragment -> {

            if (!fragment.getDependencies().isEmpty()) {
                Node start = Node.addIfAbsent(fragment.getStart(), allNodes);
                Node other = Node.addIfAbsent(fragment.getDependencies().iterator().next(), allNodes);

                start.getFragmentsWithDependency().add(fragment);
                other.getDependants().add(fragment);

                // check whether it's value fragment
                if (fragment.getEquivalentFragmentSet().fragments().size() == 1) {
                    // as value fragment is not symmetric, we need to add it again
                    other.getFragmentsWithDependency().add(fragment);
                    start.getDependants().add(fragment);
                }
            }
//            Set<Var> fragmentVarsSet = Sets.newHashSet(fragment.getVariableNames());
//            if (!fragment.getDependencies().isEmpty() && fragment.getEquivalentFragmentSet().fragments().size() == 1) {
//                fragmentVarsSet.addAll(fragment.getDependencies());
//            }
            Set<String> fragmentVarNameSet = fragment.getVariableNames().stream()
                    .map(Var::getValue).collect(Collectors.toSet());

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

//        HashSet<Node> children = Sets.newHashSet(arborescence.getParents().keySet());
//        HashSet<Node> parents = Sets.newHashSet(arborescence.getParents().values());
//
//        System.out.println("parents.size() = " + parents.size());
//        System.out.println("children.size() = " + children.size());
//        parents.removeAll(children);
//        System.out.println("parents.size() = " + parents.size());
//        System.out.println("parent = " + parents.iterator().next());

        Node root = arborescence.getRoot();
//        System.out.println("root = " + root);

        Set<Node> reachableNodes = Sets.newHashSet(root);
        while (!reachableNodes.isEmpty()) {
            Node nodeWithMinCost = reachableNodes.stream().min(Comparator.comparingDouble(node ->
                    getEdgeFragmentCost(node, arborescence, edgeFragmentChildToParent))).get();

//            System.out.println("nodeWithMinCost = " + nodeWithMinCost);

            // add edge fragment first, then node fragments
            getEdgeFragment(nodeWithMinCost, arborescence, edgeFragmentChildToParent).ifPresent(plan::add);
//            getEdgeFragment(nodeWithMinCost, arborescence, edgeFragmentChildToParent)
//                    .ifPresent(fragment -> System.out.println("fragment exist= " + fragment));
//            System.out.println("edge traversal");
            addNodeFragmentToPlan(nodeWithMinCost, plan, nodes, true);

            reachableNodes.remove(nodeWithMinCost);
            if (edgesParentToChild.containsKey(nodeWithMinCost)) {
                reachableNodes.addAll(edgesParentToChild.get(nodeWithMinCost));
            }
//            System.out.println();
        }
    }

    private static void addNodeFragmentToPlan(Node node, List<Fragment> plan, Map<String, Node> nodes,
                                              boolean visited) {
//        System.out.println("node = " + node);
        if (!visited) {
            node.getFragmentsWithoutDependency().stream()
                    .min(Comparator.comparingDouble(fragment -> fragment.fragmentCost(0)))
                    .ifPresent(firstNodeFragment -> {
                        plan.add(firstNodeFragment);
                        node.getFragmentsWithoutDependency().remove(firstNodeFragment);
                    });
        }
        node.getFragmentsWithoutDependency().addAll(node.getFragmentsWithDependencyVisited());
        plan.addAll(node.getFragmentsWithoutDependency().stream()
                .sorted(Comparator.comparingDouble(fragment -> fragment.fragmentCost(0)))
                .collect(Collectors.toList()));

        node.getFragmentsWithoutDependency().clear();
        node.getFragmentsWithDependencyVisited().clear();

        if (!node.getFragmentsWithDependency().isEmpty()) {
//            System.out.println("node.getFragmentsWithDependency() = " + node.getFragmentsWithDependency());
            node.getDependants().forEach(fragment -> {
                Node otherNode = nodes.get(fragment.getStart().getValue());
//                Node otherNode = Node.addIfAbsent(fragment.getStart(), nodes);
//                System.out.println("otherNode = " + otherNode);
                if (node.equals(otherNode)) {
                    otherNode = nodes.get(fragment.getDependencies().iterator().next().getValue());
//                    otherNode = Node.addIfAbsent(fragment.getDependencies().iterator().next(), nodes);
                }
//                System.out.println("otherNode = " + otherNode);
                otherNode.getFragmentsWithDependencyVisited().add(fragment);
                otherNode.getFragmentsWithDependency().remove(fragment);
            });

            //TODO: Do we really need to clear this?
            node.getFragmentsWithDependency().clear();
        }
        //TODO: Do we really need to clear this?
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
