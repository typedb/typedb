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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import grakn.common.util.Pair;
import grakn.core.core.JanusTraversalSourceProvider;
import grakn.core.graql.planning.gremlin.fragment.InIsaFragment;
import grakn.core.graql.planning.gremlin.fragment.InSubFragment;
import grakn.core.graql.planning.gremlin.fragment.LabelFragment;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.executor.property.PropertyExecutorFactory;
import grakn.core.kb.graql.planning.Arborescence;
import grakn.core.kb.graql.planning.ChuLiuEdmonds;
import grakn.core.kb.graql.planning.GraknQueryPlannerException;
import grakn.core.kb.graql.planning.gremlin.EquivalentFragmentSet;
import grakn.core.kb.graql.planning.gremlin.Fragment;
import grakn.core.kb.graql.planning.gremlin.GraqlTraversal;
import grakn.core.kb.graql.planning.gremlin.TraversalPlanFactory;
import grakn.core.kb.graql.planning.spanningtree.graph.DirectedEdge;
import grakn.core.kb.graql.planning.spanningtree.graph.Node;
import grakn.core.kb.graql.planning.spanningtree.graph.NodeId;
import grakn.core.kb.graql.planning.spanningtree.graph.SparseWeightedGraph;
import grakn.core.kb.graql.planning.spanningtree.util.Weighted;
import grakn.core.kb.keyspace.KeyspaceStatistics;
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

import static grakn.core.graql.planning.NodesUtil.buildNodesWithDependencies;
import static grakn.core.graql.planning.NodesUtil.nodeVisitedDependenciesFragments;
import static grakn.core.graql.planning.RelationTypeInference.inferRelationTypes;

/**
 * Class for generating greedy traversal plans
 */
public class TraversalPlanFactoryImpl implements TraversalPlanFactory {

    private static final Logger LOG = LoggerFactory.getLogger(TraversalPlanFactoryImpl.class);

    private static final int MAX_STARTING_POINTS = 3;
    private final JanusTraversalSourceProvider janusTraversalSourceProvider;
    private final ConceptManager conceptManager;
    private PropertyExecutorFactory propertyExecutorFactory;
    private final long shardingThreshold;
    private final KeyspaceStatistics keyspaceStatistics;

    public TraversalPlanFactoryImpl(JanusTraversalSourceProvider janusTraversalSourceProvider, ConceptManager conceptManager,
                                    PropertyExecutorFactory propertyExecutorFactory, long shardingThreshold,
                                    KeyspaceStatistics keyspaceStatistics) {
        this.janusTraversalSourceProvider = janusTraversalSourceProvider;
        this.conceptManager = conceptManager;
        this.propertyExecutorFactory = propertyExecutorFactory;
        this.shardingThreshold = shardingThreshold;
        this.keyspaceStatistics = keyspaceStatistics;
    }

    /**
     * Create a traversal plan.
     *
     * @param pattern a pattern to find a query plan for
     * @return a semi-optimal traversal plan
     */
    public GraqlTraversal createTraversal(Pattern pattern) {
        Collection<Conjunction<Statement>> patterns = pattern.getDisjunctiveNormalForm().getPatterns();

        Set<List<? extends Fragment>> fragments = patterns.stream()
                .map(conjunction -> new ConjunctionQuery(conjunction, conceptManager, propertyExecutorFactory))
                .map(this::planForConjunction)
                .collect(ImmutableSet.toImmutableSet());

        return new GraqlTraversalImpl(janusTraversalSourceProvider, conceptManager, fragments);
    }

    /**
     * Create a plan using Edmonds' algorithm with greedy approach to execute a single conjunction
     *
     * @param query the conjunction query to find a traversal plan
     * @return a semi-optimal traversal plan to execute the given conjunction
     */
    private List<Fragment> planForConjunction(ConjunctionQuery query) {
        // a query plan is an ordered list of fragments
        List<Fragment> plan = new ArrayList<>();

        // flatten all the possible fragments from the conjunction query (these become edges in the query graph)
        Set<Fragment> allFragments = query.getEquivalentFragmentSets().stream()
                .flatMap(EquivalentFragmentSet::stream).collect(Collectors.toSet());

        // if role players' types are known, we can infer the types of the relation, adding label & isa fragments
        Set<Fragment> inferredFragments = inferRelationTypes(conceptManager, allFragments);
        allFragments.addAll(inferredFragments);

        // convert fragments into nodes - some fragments create virtual middle nodes to ensure the Janus edge is traversed
        ImmutableMap<NodeId, Node> queryGraphNodes = buildNodesWithDependencies(allFragments);

        // it's possible that some (or all) fragments are disconnected, e.g. $x isa person; $y isa dog;
        Collection<Set<Fragment>> connectedFragmentSets = getConnectedFragmentSets(allFragments);

        // build a query plan for each query subgraph separately
        for (Set<Fragment> connectedFragments : connectedFragmentSets) {
            // one of two cases - either we have a connected graph > 1 node, which is used to compute a MST, OR exactly 1 node
            Arborescence<Node> subgraphArborescence = computeArborescence(connectedFragments, queryGraphNodes);
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
                    throw GraknQueryPlannerException.create("Query planner exception - expected one unhandled node, found " + unhandledNodes.size());
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


    private Arborescence<Node> computeArborescence(Set<Fragment> connectedFragments, ImmutableMap<NodeId, Node> nodes) {
        Map<Node, Double> nodesWithFixedCost = new HashMap<>();

        connectedFragments.forEach(fragment -> {
            if (fragment.hasFixedFragmentCost()) {
                NodeId startNodeId = Iterators.getOnlyElement(fragment.getNodes().iterator()).getNodeId();
                Node startNode = nodes.get(startNodeId);
                nodesWithFixedCost.put(startNode, getLogInstanceCount(fragment));
                startNode.setFixedFragmentCost(fragment.fragmentCost());
            }
        });

        // update cost of reaching subtypes from an indexed supertyp
        updateFixedCostSubsReachableByIndex(nodes, nodesWithFixedCost, connectedFragments);

        // fragments that represent Janus edges
        Set<Fragment> edgeFragmentSet = new HashSet<>();

        // save the fragments corresponding to edges, and updates some costs if we can via shard count
        for (Fragment fragment : connectedFragments) {
            if (fragment.end() != null) {
                edgeFragmentSet.add(fragment);
                // update the cost of an `InIsa` Fragment if we have some estimated cost
                if (fragment instanceof InIsaFragment) {
                    Node type = nodes.get(NodeId.of(NodeId.Type.VAR, fragment.start()));
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
                    .map(arborescenceInside -> arborescenceInside.val)
                    .orElse(Arborescence.empty());

            return arborescence;
        } else {
            return null;
        }
    }

    private static Set<Weighted<DirectedEdge>> buildWeightedGraph(Map<NodeId, Node> allNodes,
                                                                  Set<Fragment> edgeFragmentSet) {

        Set<Weighted<DirectedEdge>> weightedGraph = new HashSet<>();
        // add each edge together with its weight
        edgeFragmentSet.stream()
                .flatMap(fragment -> fragment.directedEdges(allNodes).stream())
                .forEach(weightedDirectedEdge -> weightedGraph.add(weightedDirectedEdge));
        return weightedGraph;
    }

    private Set<Node> chooseStartingNodeSet(Set<Fragment> fragmentSet, Map<NodeId, Node> allNodes, SparseWeightedGraph sparseWeightedGraph) {
        Set<Node> highPriorityStartingNodeSet = new HashSet<>();
        Set<Node> lowPriorityStartingNodeSet = new HashSet<>();

        fragmentSet.stream()
                .filter(Fragment::hasFixedFragmentCost)
                .sorted(Comparator.comparing(fragment -> fragment.estimatedCostAsStartingPoint(conceptManager, keyspaceStatistics)))
                .limit(MAX_STARTING_POINTS)
                .forEach(fragment -> {
                    Node node = allNodes.get(NodeId.of(NodeId.Type.VAR, fragment.start()));
                    //TODO: this behaviour should be incorporated into the MST weight calculation
                    if (fragment instanceof LabelFragment) {
                        Type type = conceptManager.getType(Iterators.getOnlyElement(((LabelFragment) fragment).labels().iterator()));
                        if (type != null && type.isImplicit()) {
                            // implicit types have low priority because their instances may be edges
                            lowPriorityStartingNodeSet.add(node);
                        } else {
                            // other labels/types are the ideal starting point as they are indexed
                            highPriorityStartingNodeSet.add(node);
                        }
                    } else {
                        highPriorityStartingNodeSet.add(node);
                    }
                });

        Set<Node> startingNodes;
        if (!highPriorityStartingNodeSet.isEmpty()) {
            startingNodes = highPriorityStartingNodeSet;
        } else {
            // if we have no good starting points, use any valid nodes
            startingNodes = !lowPriorityStartingNodeSet.isEmpty() ?
                    lowPriorityStartingNodeSet :
                    sparseWeightedGraph.getNodes().stream().filter(Node::isValidStartingPoint).collect(Collectors.toSet());
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
        Map<Integer, Set<Variable>> varSetMap = new HashMap<>();
        Map<Integer, Set<Fragment>> fragmentSetMap = new HashMap<>();
        int[] index = {0};
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

    private double getLogInstanceCount(Fragment fragment) {
        // set the weight of the node as a starting point based on log(number of this node)
        double logInstanceCount;

        //this is to make sure we don't end up with log(0)
        double shardLoadFactor = 0.25;
        if (fragment instanceof LabelFragment) {
            // only LabelFragment (corresponding to type vertices) can be sharded
            LabelFragment labelFragment = (LabelFragment)fragment;
            Label label = Iterators.getOnlyElement(labelFragment.labels().iterator());

            //TODO: this manipulation is to retain the previous behaviour, we need to update the query planner
            //to remove the sharding threshold dependency and make this more granular
            double instanceCount = (conceptManager.getSchemaConcept(label).subs()
                    .mapToLong(schemaConcept -> keyspaceStatistics.count(conceptManager, schemaConcept.label()))
                    .sum() / shardingThreshold + shardLoadFactor ) * shardingThreshold;
            logInstanceCount = Math.log(instanceCount);
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
                Node superType = allNodes.get(NodeId.of(NodeId.Type.VAR, fragment.start()));
                if (nodesWithFixedCost.containsKey(superType) && nodesWithFixedCost.get(superType) > 0D) {
                    Node subType = allNodes.get(NodeId.of(NodeId.Type.VAR, fragment.end()));
                    return !nodesWithFixedCost.containsKey(subType);
                }
            }
            return false;
        }).collect(Collectors.toSet());

        if (!validSubFragments.isEmpty()) {
            validSubFragments.forEach(fragment -> {
                // TODO: should decrease the weight of sub type after each level
                nodesWithFixedCost.put(allNodes.get(NodeId.of(NodeId.Type.VAR, fragment.end())),
                        nodesWithFixedCost.get(allNodes.get(NodeId.of(NodeId.Type.VAR, fragment.start()))));
            });
            // recursively process all the sub fragments
            updateFixedCostSubsReachableByIndex(allNodes, nodesWithFixedCost, fragments);
        }
    }

    private static Map<Node, Map<Node, Fragment>> virtualMiddleNodeToFragmentMapping(Set<Fragment> connectedFragments, Map<NodeId, Node> nodes) {
        Map<Node, Map<Node, Fragment>> middleNodeFragmentMapping = new HashMap<>();
        for (Fragment fragment : connectedFragments) {
            Pair<Node, Node> middleNodeDirectedEdge = fragment.getMiddleNodeDirectedEdge(nodes);
            if (middleNodeDirectedEdge != null) {
                middleNodeFragmentMapping.putIfAbsent(middleNodeDirectedEdge.first(), new HashMap<>());
                middleNodeFragmentMapping.get(middleNodeDirectedEdge.first()).put(middleNodeDirectedEdge.second(), fragment);
            }
        }
        return middleNodeFragmentMapping;
    }

}
