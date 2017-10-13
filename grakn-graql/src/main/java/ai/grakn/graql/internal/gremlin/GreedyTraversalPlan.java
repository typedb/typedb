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

import ai.grakn.GraknTx;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;
import ai.grakn.graql.internal.gremlin.spanningtree.Arborescence;
import ai.grakn.graql.internal.gremlin.spanningtree.ChuLiuEdmonds;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.DirectedEdge;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.Node;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.NodeId;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.SparseWeightedGraph;
import ai.grakn.graql.internal.gremlin.spanningtree.util.Weighted;
import ai.grakn.graql.internal.pattern.property.ValueProperty;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static ai.grakn.util.CommonUtil.toImmutableSet;

/**
 * Class for generating greedy traversal plans
 *
 * @author Felix Chapman
 * @author Jason Liu
 */
public class GreedyTraversalPlan {

    protected static final Logger LOG = LoggerFactory.getLogger(GreedyTraversalPlan.class);

    /**
     * Create a traversal plan.
     *
     * @param pattern a pattern to find a query plan for
     * @return a semi-optimal traversal plan
     */
    public static GraqlTraversal createTraversal(PatternAdmin pattern, GraknTx graph) {
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
        Map<NodeId, Node> allNodes = new HashMap<>();

        Collection<Set<Fragment>> connectedFragmentSets = getConnectedFragmentSets(query, allNodes);

        connectedFragmentSets.forEach(fragmentSet -> {

            Set<Node> connectedNodes = new HashSet<>();
            Map<Node, Map<Node, Fragment>> edges = new HashMap<>();
            final Set<Node> nodesWithFixedCost = new HashSet<>();
            Set<Weighted<DirectedEdge<Node>>> weightedGraph = new HashSet<>();

            fragmentSet.stream()
                    .filter(filterNodeFragment(plan, allNodes, connectedNodes, nodesWithFixedCost))
                    .flatMap(fragment -> fragment.directedEdges(allNodes, edges).stream())
                    .forEach(weightedDirectedEdge -> {
                        weightedGraph.add(weightedDirectedEdge);
                        connectedNodes.add(weightedDirectedEdge.val.destination);
                        connectedNodes.add(weightedDirectedEdge.val.source);
                    });

            // if there is no edge fragment
            if (!weightedGraph.isEmpty()) {
                SparseWeightedGraph<Node> sparseWeightedGraph = SparseWeightedGraph.from(weightedGraph);

                final Collection<Node> startingNodes = nodesWithFixedCost.isEmpty() ?
                        sparseWeightedGraph.getNodes().stream()
                                .filter(Node::isValidStartingPoint).collect(Collectors.toSet()) : nodesWithFixedCost;

                Arborescence<Node> arborescence = startingNodes.stream()
                        .map(node -> ChuLiuEdmonds.getMaxArborescence(sparseWeightedGraph, node))
                        .max(Comparator.comparingDouble(tree -> tree.weight))
                        .map(arborescenceInside -> arborescenceInside.val).orElse(Arborescence.empty());

                greedyTraversal(plan, arborescence, allNodes, edges);
            }

            addUnvisitedNodeFragments(plan, allNodes, connectedNodes);
        });

        addUnvisitedNodeFragments(plan, allNodes, allNodes.values());

        LOG.trace("Greedy Plan = " + plan);
        return plan;
    }

    private static void addUnvisitedNodeFragments(List<Fragment> plan,
                                                  Map<NodeId, Node> allNodes,
                                                  Collection<Node> connectedNodes) {
        Set<Node> nodeWithFragment = connectedNodes.stream()
                .filter(node -> !node.getFragmentsWithoutDependency().isEmpty() ||
                        !node.getFragmentsWithDependencyVisited().isEmpty())
                .collect(Collectors.toSet());
        while (!nodeWithFragment.isEmpty()) {
            nodeWithFragment.forEach(node -> addNodeFragmentToPlan(node, plan, allNodes, false));
            nodeWithFragment = connectedNodes.stream()
                    .filter(node -> !node.getFragmentsWithoutDependency().isEmpty() ||
                            !node.getFragmentsWithDependencyVisited().isEmpty())
                    .collect(Collectors.toSet());
        }
    }

    private static Predicate<Fragment> filterNodeFragment(List<Fragment> plan, Map<NodeId, Node> allNodes,
                                                          Set<Node> connectedNodes, Set<Node> nodesWithFixedCost) {
        return fragment -> {
            if (fragment.end() == null) {
                Node start = Node.addIfAbsent(NodeId.NodeType.VAR, fragment.start(), allNodes);
                connectedNodes.add(start);

                if (fragment.hasFixedFragmentCost()) {
                    // fragments that should be done right away
                    plan.add(fragment);
                    nodesWithFixedCost.add(start);
                    start.setFixedFragmentCost(fragment.fragmentCost());

                } else if (fragment.dependencies().isEmpty()) {
                    //fragments that should be done when a node has been visited
                    start.getFragmentsWithoutDependency().add(fragment);

                }
                return false;

            } else {
                return true;
            }
        };
    }

    private static Collection<Set<Fragment>> getConnectedFragmentSets(ConjunctionQuery query,
                                                                      Map<NodeId, Node> allNodes) {
        Map<Integer, Set<Var>> varSetMap = new HashMap<>();
        Map<Integer, Set<Fragment>> fragmentSetMap = new HashMap<>();
        final int[] index = {0};
        query.getEquivalentFragmentSets().stream().flatMap(EquivalentFragmentSet::stream).forEach(fragment -> {

            if (!fragment.dependencies().isEmpty()) {
                // it's either neq or value fragment

                Node start = Node.addIfAbsent(NodeId.NodeType.VAR, fragment.start(), allNodes);
                Node other = Node.addIfAbsent(NodeId.NodeType.VAR, fragment.dependencies().iterator().next(),
                        allNodes);

                start.getFragmentsWithDependency().add(fragment);
                other.getDependants().add(fragment);

                // check whether it's value fragment
                if (fragment.varProperty() instanceof ValueProperty) {
                    // as value fragment is not symmetric, we need to add it again
                    other.getFragmentsWithDependency().add(fragment);
                    start.getDependants().add(fragment);
                }
            }

            Set<Var> fragmentVarNameSet = Sets.newHashSet(fragment.vars());
            List<Integer> setsWithVarInCommon = new ArrayList<>();
            varSetMap.forEach((setIndex, varNameSet) -> {
                if (!Collections.disjoint(varNameSet, fragmentVarNameSet)) {
                    setsWithVarInCommon.add(setIndex);
                }
            });

            if (setsWithVarInCommon.isEmpty()) {
                index[0] += 1;
                varSetMap.put(index[0], fragmentVarNameSet);
                fragmentSetMap.put(index[0], Sets.newHashSet(fragment));
            } else {
                Iterator<Integer> iterator = setsWithVarInCommon.iterator();
                Integer firstSet = iterator.next();
                varSetMap.get(firstSet).addAll(fragmentVarNameSet);
                fragmentSetMap.get(firstSet).add(fragment);
                while (iterator.hasNext()) {
                    Integer nextSet = iterator.next();
                    varSetMap.get(firstSet).addAll(varSetMap.remove(nextSet));
                    fragmentSetMap.get(firstSet).addAll(fragmentSetMap.remove(nextSet));
                }
            }
        });
        return fragmentSetMap.values();
    }

    private static void greedyTraversal(List<Fragment> plan, Arborescence<Node> arborescence,
                                        Map<NodeId, Node> nodes,
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
            Node nodeWithMinCost = reachableNodes.stream().min(Comparator.comparingDouble(node ->
                    branchWeight(node, arborescence, edgesParentToChild, edgeFragmentChildToParent))).get();

            // add edge fragment first, then node fragments
            getEdgeFragment(nodeWithMinCost, arborescence, edgeFragmentChildToParent).ifPresent(plan::add);
            addNodeFragmentToPlan(nodeWithMinCost, plan, nodes, true);

            reachableNodes.remove(nodeWithMinCost);
            if (edgesParentToChild.containsKey(nodeWithMinCost)) {
                reachableNodes.addAll(edgesParentToChild.get(nodeWithMinCost));
            }
        }
    }

    private static double branchWeight(Node node, Arborescence<Node> arborescence,
                                       Map<Node, Set<Node>> edgesParentToChild,
                                       Map<Node, Map<Node, Fragment>> edgeFragmentChildToParent) {
        if (!node.getNodeWeight().isPresent()) {
            node.setNodeWeight(Optional.of(
                    getEdgeFragmentCost(node, arborescence, edgeFragmentChildToParent) + nodeFragmentWeight(node)));
        }
        if (!node.getBranchWeight().isPresent()) {
            final double[] weight = {node.getNodeWeight().get()};
            if (edgesParentToChild.containsKey(node)) {
                edgesParentToChild.get(node).forEach(child ->
                        weight[0] += branchWeight(child, arborescence, edgesParentToChild, edgeFragmentChildToParent));
            }
            node.setBranchWeight(Optional.of(weight[0]));
        }
        return node.getBranchWeight().get();
    }

    private static double nodeFragmentWeight(Node node) {
        double costFragmentsWithoutDependency = node.getFragmentsWithoutDependency().stream()
                .mapToDouble(Fragment::fragmentCost).sum();
        double costFragmentsWithDependencyVisited = node.getFragmentsWithDependencyVisited().stream()
                .mapToDouble(Fragment::fragmentCost).sum();
        double costFragmentsWithDependency = node.getFragmentsWithDependency().stream()
                .mapToDouble(Fragment::fragmentCost).sum();
        return costFragmentsWithoutDependency + node.getFixedFragmentCost() +
                (costFragmentsWithDependencyVisited + costFragmentsWithDependency) / 2D;
    }

    private static void addNodeFragmentToPlan(Node node, List<Fragment> plan, Map<NodeId, Node> nodes,
                                              boolean visited) {
        if (!visited) {
            node.getFragmentsWithoutDependency().stream()
                    .min(Comparator.comparingDouble(Fragment::fragmentCost))
                    .ifPresent(firstNodeFragment -> {
                        plan.add(firstNodeFragment);
                        node.getFragmentsWithoutDependency().remove(firstNodeFragment);
                    });
        }
        node.getFragmentsWithoutDependency().addAll(node.getFragmentsWithDependencyVisited());
        plan.addAll(node.getFragmentsWithoutDependency().stream()
                .sorted(Comparator.comparingDouble(Fragment::fragmentCost))
                .collect(Collectors.toList()));

        node.getFragmentsWithoutDependency().clear();
        node.getFragmentsWithDependencyVisited().clear();

        if (!node.getFragmentsWithDependency().isEmpty()) {
            node.getDependants().forEach(fragment -> {
                Node otherNode = nodes.get(new NodeId(NodeId.NodeType.VAR, fragment.start()));
                if (node.equals(otherNode)) {
                    otherNode = nodes.get(new NodeId(NodeId.NodeType.VAR, fragment.dependencies().iterator().next()));
                }
                otherNode.getFragmentsWithDependencyVisited().add(fragment);
                otherNode.getFragmentsWithDependency().remove(fragment);
            });
            node.getFragmentsWithDependency().clear();
        }
        node.getDependants().clear();
    }

    private static double getEdgeFragmentCost(Node node, Arborescence<Node> arborescence,
                                              Map<Node, Map<Node, Fragment>> edgeToFragment) {
        Optional<Fragment> fragment = getEdgeFragment(node, arborescence, edgeToFragment);
        return fragment.map(Fragment::fragmentCost).orElse(0D);
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
