/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package grakn.core.graql.gremlin;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import grakn.core.graql.gremlin.fragment.Fragment;
import grakn.core.graql.gremlin.fragment.InIsaFragment;
import grakn.core.graql.gremlin.fragment.InSubFragment;
import grakn.core.graql.gremlin.fragment.LabelFragment;
import grakn.core.graql.gremlin.spanningtree.Arborescence;
import grakn.core.graql.gremlin.spanningtree.ChuLiuEdmonds;
import grakn.core.graql.gremlin.spanningtree.graph.DirectedEdge;
import grakn.core.graql.gremlin.spanningtree.graph.Node;
import grakn.core.graql.gremlin.spanningtree.graph.NodeId;
import grakn.core.graql.gremlin.spanningtree.graph.SparseWeightedGraph;
import grakn.core.graql.gremlin.spanningtree.util.Weighted;
import grakn.core.graql.reasoner.utils.Pair;
import grakn.core.server.exception.GraknServerException;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.core.common.util.CommonUtil.toImmutableSet;
import static grakn.core.graql.gremlin.NodesUtil.buildNodesWithDependencies;
import static grakn.core.graql.gremlin.NodesUtil.nodeVisitedDependenciesFragments;
import static grakn.core.graql.gremlin.RelationTypeInference.inferRelationTypes;
import static grakn.core.graql.gremlin.fragment.Fragment.SHARD_LOAD_FACTOR;

/**
 * Class for generating greedy traversal plans
 */
public class TraversalPlanner {

    protected static final Logger LOG = LoggerFactory.getLogger(TraversalPlanner.class);

    /**
     * Create a traversal plan.
     *
     * @param pattern a pattern to find a query plan for
     * @return a semi-optimal traversal plan
     */
    public static GraqlTraversal createTraversal(Pattern pattern, TransactionOLTP tx) {
        Collection<Conjunction<Statement>> patterns = pattern.getDisjunctiveNormalForm().getPatterns();

        Set<? extends List<Fragment>> fragments = patterns.stream()
                .map(conjunction -> new ConjunctionQuery(conjunction, tx))
                .map((ConjunctionQuery query) -> planForConjunction(query, tx))
                .collect(toImmutableSet());

        return GraqlTraversal.create(fragments);
    }

    /**
     * Create a plan using Edmonds' algorithm with greedy approach to execute a single conjunction
     *
     * @param query the conjunction query to find a traversal plan
     * @return a semi-optimal traversal plan to execute the given conjunction
     */
    private static List<Fragment> planForConjunction(ConjunctionQuery query, TransactionOLTP tx) {
        // a query plan is an ordered list of fragments
        final List<Fragment> plan = new ArrayList<>();

        // flatten all the possible fragments from the conjunction query (these become edges in the query graph)
        final Set<Fragment> allFragments = query.getEquivalentFragmentSets().stream()
                .flatMap(EquivalentFragmentSet::stream).collect(Collectors.toSet());

        // if role players' types are known, we can infer the types of the relation, adding label & isa fragments
        Set<Fragment> inferredFragments = inferRelationTypes(tx, allFragments);
        allFragments.addAll(inferredFragments);

        // convert fragments into nodes - some fragments create virtual middle nodes to ensure the Janus edge is traversed
        ImmutableMap<NodeId, Node> queryGraphNodes = buildNodesWithDependencies(allFragments);

        // it's possible that some (or all) fragments are disconnected, e.g. $x isa person; $y isa dog;
        Collection<Set<Fragment>> connectedFragmentSets = getConnectedFragmentSets(allFragments);

        // build a query plan for each query subgraph separately
        for (Set<Fragment> connectedFragments : connectedFragmentSets) {
            // one of two cases - either we have a connected graph > 1 node, which is used to compute a MST, OR exactly 1 node
            Arborescence<Node> subgraphArborescence = computeArborescence(connectedFragments, queryGraphNodes, tx);
            if (subgraphArborescence != null) {
                // collect the mapping from directed edge back to fragments -- inverse operation of creating virtual middle nodes
                Map<Node, Map<Node, Fragment>> middleNodeFragmentMapping = virtualMiddleNodeToFragmentMapping(connectedFragments, queryGraphNodes);
                List<Fragment> subplan = GreedyTreeTraversal.greedyTraversal(subgraphArborescence, queryGraphNodes, middleNodeFragmentMapping);
                plan.addAll(subplan);
            } else {
                // find and include all the nodes not touched in the MST in the plan
               Set<Node> unhandledNodes = connectedFragments.stream()
                        .flatMap(fragment -> fragment.getNodes().stream())
                        .map(node -> queryGraphNodes.get(node.getNodeId()))
                        .collect(Collectors.toSet());
               if (unhandledNodes.size() != 1) {
                   throw GraknServerException.create("Query planner exception - expected one unhandled node, found " + unhandledNodes.size());
               }
               plan.addAll(nodeVisitedDependenciesFragments(Iterators.getOnlyElement(unhandledNodes.iterator()), queryGraphNodes));
            }
        }

        // this shouldn't be necessary, but we keep it just in case of an edge case that we haven't thought of
        List<Fragment> remainingFragments = fragmentsForUnvisitedNodes(queryGraphNodes, queryGraphNodes.values());
        if (remainingFragments.size() > 0) {
            LOG.warn("Expected all fragments to be handled, but found these: " + remainingFragments);
            plan.addAll(remainingFragments);
        }

        LOG.trace("Greedy Plan = {}", plan);
        return plan;
    }


    private static Arborescence<Node> computeArborescence(Set<Fragment> connectedFragments, ImmutableMap<NodeId, Node> nodes, TransactionOLTP tx) {
        final Map<Node, Double> nodesWithFixedCost = new HashMap<>();

        connectedFragments.forEach(fragment -> {
            if (fragment.hasFixedFragmentCost()) {
                NodeId startNodeId = Iterators.getOnlyElement(fragment.getNodes().iterator()).getNodeId();
                Node startNode = nodes.get(startNodeId);
                nodesWithFixedCost.put(startNode, getLogInstanceCount(tx, fragment));
                startNode.setFixedFragmentCost(fragment.fragmentCost());
            }
        });

        // update cost of reaching subtypes from an indexed supertyp
        updateFixedCostSubsReachableByIndex(nodes, nodesWithFixedCost, connectedFragments);

        // fragments that represent Janus edges
        final Set<Fragment> edgeFragmentSet = new HashSet<>();

        // save the fragments corresponding to edges, and updates some costs if we can via shard count
        for (Fragment fragment : connectedFragments) {
            if (fragment.end() != null) {
                edgeFragmentSet.add(fragment);
                // update the cost of an `InIsa` Fragment if we have some estimated cost
                if (fragment instanceof InIsaFragment) {
                    Node type = nodes.get(NodeId.of(NodeId.NodeType.VAR, fragment.start()));
                    if (nodesWithFixedCost.containsKey(type) && nodesWithFixedCost.get(type) > 0) {
                        fragment.setAccurateFragmentCost(nodesWithFixedCost.get(type));
                    }
                }
            }
        }

        // convert fragments to a connected graph
        Set<Weighted<DirectedEdge>> weightedGraph = buildWeightedGraph(nodes, edgeFragmentSet);

        if (!weightedGraph.isEmpty()) {
            // sparse graph for better performance
            SparseWeightedGraph sparseWeightedGraph = SparseWeightedGraph.from(weightedGraph);
            Set<Node> startingNodes = chooseStartingNodeSet(connectedFragments, nodes, sparseWeightedGraph);

            // find the minimum spanning tree for each root
            // then get the tree with minimum weight
            Arborescence<Node> arborescence = startingNodes.stream()
                    .map(node -> ChuLiuEdmonds.getMaxArborescence(sparseWeightedGraph, node))
                    .max(Comparator.comparingDouble(tree -> tree.weight))
                    .map(arborescenceInside -> arborescenceInside.val).orElse(Arborescence.empty());

            return arborescence;
        } else {
            return null;
        }
    }

    private static Set<Weighted<DirectedEdge>> buildWeightedGraph(Map<NodeId, Node> allNodes,
                                                                  Set<Fragment> edgeFragmentSet) {

        final Set<Weighted<DirectedEdge>> weightedGraph = new HashSet<>();
        // add each edge together with its weight
        edgeFragmentSet.stream()
                .flatMap(fragment -> fragment.directedEdges(allNodes).stream())
                .forEach(weightedDirectedEdge -> weightedGraph.add(weightedDirectedEdge));
        return weightedGraph;
    }

    private static Set<Node> chooseStartingNodeSet(Set<Fragment> fragmentSet, Map<NodeId, Node> allNodes, SparseWeightedGraph sparseWeightedGraph) {
        final Set<Node> highPriorityStartingNodeSet = new HashSet<>();

        fragmentSet.forEach(fragment -> {
            if (fragment.hasFixedFragmentCost()) {
                Node node = allNodes.get(NodeId.of(NodeId.NodeType.VAR, fragment.start()));
                highPriorityStartingNodeSet.add(node);
            }
        });

        Set<Node> startingNodes;
        if (!highPriorityStartingNodeSet.isEmpty()) {
            startingNodes = highPriorityStartingNodeSet;
        } else {
            // if we have no good starting points, use any valid nodes
            startingNodes = sparseWeightedGraph.getNodes().stream()
                    .filter(Node::isValidStartingPoint).collect(Collectors.toSet());
        }
        return startingNodes;
    }

    // add unvisited node fragments to plan
    private static List<Fragment> fragmentsForUnvisitedNodes(Map<NodeId, Node> allNodes, Collection<Node> connectedNodes) {
        List<Fragment> subplan = new LinkedList<>();

        Set<Node> nodeWithFragment = connectedNodes.stream()
                // make sure the fragment either has no dependency or dependencies have been dealt with
                .filter(node -> !node.getFragmentsWithoutDependency().isEmpty() ||
                        !node.getFragmentsWithDependencyVisited().isEmpty())
                .collect(Collectors.toSet());
        while (!nodeWithFragment.isEmpty()) {
            nodeWithFragment.forEach(node -> subplan.addAll(nodeVisitedDependenciesFragments(node, allNodes)));
            nodeWithFragment = connectedNodes.stream()
                    .filter(node -> !node.getFragmentsWithoutDependency().isEmpty() ||
                            !node.getFragmentsWithDependencyVisited().isEmpty())
                    .collect(Collectors.toSet());
        }

        return subplan;
    }

    // return a collection of set, in each set, all the fragments are connected
    private static Collection<Set<Fragment>> getConnectedFragmentSets(Set<Fragment> allFragments) {
        // TODO this could be implemented in a more readable way (eg. using a graph + BFS etc.)
        final Map<Integer, Set<Variable>> varSetMap = new HashMap<>();
        final Map<Integer, Set<Fragment>> fragmentSetMap = new HashMap<>();
        final int[] index = {0};
        allFragments.forEach(fragment -> {
            Set<Variable> fragmentVarNameSet = Sets.newHashSet(fragment.vars());
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

    private static double getLogInstanceCount(TransactionOLTP tx, Fragment fragment) {
        // set the weight of the node as a starting point based on log(number of this node)
        double logInstanceCount;
        if (fragment instanceof LabelFragment) {
            // only LabelFragment (corresponding to type vertices) can be sharded
            Long shardCount = ((LabelFragment) fragment).getShardCount(tx);
            logInstanceCount = Math.log(shardCount - 1D + SHARD_LOAD_FACTOR) +
                    Math.log(tx.shardingThreshold());
        } else {
            logInstanceCount = -1D;
        }
        return logInstanceCount;
    }


    // if in-sub starts from an indexed supertype, update the fragment cost of in-isa starting from the subtypes
    private static void updateFixedCostSubsReachableByIndex(ImmutableMap<NodeId, Node> allNodes,
                                                            Map<Node, Double> nodesWithFixedCost,
                                                            Set<Fragment> fragments) {

        Set<Fragment> validSubFragments = fragments.stream().filter(fragment -> {
            if (fragment instanceof InSubFragment) {
                Node superType = allNodes.get(NodeId.of(NodeId.NodeType.VAR, fragment.start()));
                if (nodesWithFixedCost.containsKey(superType) && nodesWithFixedCost.get(superType) > 0D) {
                    Node subType = allNodes.get(NodeId.of(NodeId.NodeType.VAR, fragment.end()));
                    return !nodesWithFixedCost.containsKey(subType);
                }
            }
            return false;
        }).collect(Collectors.toSet());

        if (!validSubFragments.isEmpty()) {
            validSubFragments.forEach(fragment -> {
                // TODO: should decrease the weight of sub type after each level
                nodesWithFixedCost.put(allNodes.get(NodeId.of(NodeId.NodeType.VAR, fragment.end())),
                        nodesWithFixedCost.get(allNodes.get(NodeId.of(NodeId.NodeType.VAR, fragment.start()))));
            });
            // recursively process all the sub fragments
            updateFixedCostSubsReachableByIndex(allNodes, nodesWithFixedCost, fragments);
        }
    }

    static Map<Node, Map<Node, Fragment>> virtualMiddleNodeToFragmentMapping(Set<Fragment> connectedFragments, Map<NodeId, Node> nodes) {
        Map<Node, Map<Node, Fragment>> middleNodeFragmentMapping = new HashMap<>();
        for (Fragment fragment : connectedFragments) {
            Pair<Node, Node> middleNodeDirectedEdge = fragment.getMiddleNodeDirectedEdge(nodes);
            if (middleNodeDirectedEdge != null) {
                middleNodeFragmentMapping.putIfAbsent(middleNodeDirectedEdge.getKey(), new HashMap<>());
                middleNodeFragmentMapping.get(middleNodeDirectedEdge.getKey()).put(middleNodeDirectedEdge.getValue(), fragment);
            }
        }
        return middleNodeFragmentMapping;
    }

}
