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

package grakn.core.graql.internal.gremlin;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.RelationType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.concept.Type;
import grakn.core.graql.internal.gremlin.fragment.Fragment;
import grakn.core.graql.internal.gremlin.fragment.Fragments;
import grakn.core.graql.internal.gremlin.fragment.InIsaFragment;
import grakn.core.graql.internal.gremlin.fragment.InSubFragment;
import grakn.core.graql.internal.gremlin.fragment.LabelFragment;
import grakn.core.graql.internal.gremlin.fragment.OutRolePlayerFragment;
import grakn.core.graql.internal.gremlin.fragment.ValueFragment;
import grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets;
import grakn.core.graql.internal.gremlin.spanningtree.Arborescence;
import grakn.core.graql.internal.gremlin.spanningtree.ChuLiuEdmonds;
import grakn.core.graql.internal.gremlin.spanningtree.graph.DirectedEdge;
import grakn.core.graql.internal.gremlin.spanningtree.graph.Node;
import grakn.core.graql.internal.gremlin.spanningtree.graph.NodeId;
import grakn.core.graql.internal.gremlin.spanningtree.graph.SparseWeightedGraph;
import grakn.core.graql.internal.gremlin.spanningtree.util.Weighted;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.property.IsaProperty;
import graql.lang.property.TypeProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.core.common.util.CommonUtil.toImmutableSet;
import static grakn.core.graql.internal.gremlin.fragment.Fragment.SHARD_LOAD_FACTOR;
import static graql.lang.Graql.var;

/**
 * Class for generating greedy traversal plans
 *
 */
public class GreedyTraversalPlan {

    protected static final Logger LOG = LoggerFactory.getLogger(GreedyTraversalPlan.class);

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

        final List<Fragment> plan = new ArrayList<>(); // this will be the final plan
        final Map<NodeId, Node> allNodes = new HashMap<>(); // all the nodes in the spanning tree

        final Set<Node> connectedNodes = new HashSet<>();
        final Map<Node, Double> nodesWithFixedCost = new HashMap<>();

        // getting all the fragments from the conjunction query
        final Set<Fragment> allFragments = query.getEquivalentFragmentSets().stream()
                .flatMap(EquivalentFragmentSet::stream).collect(Collectors.toSet());

        // if role p[ayers' types are known, we can infer the types of the relationship
        // then add a label fragment to the fragment set
        inferRelationshipTypes(tx, allFragments);

        // it's possible that some (or all) fragments are disconnect
        // e.g. $x isa person; $y isa dog;
        // these are valid conjunctions and useful when inserting new data
        Collection<Set<Fragment>> connectedFragmentSets =
                getConnectedFragmentSets(plan, allFragments, allNodes, connectedNodes,  nodesWithFixedCost, tx);

        // generate plan for each connected set of fragments
        // since each set is independent, order of the set doesn't matter
        connectedFragmentSets.forEach(fragmentSet -> {

            final Map<Node, Map<Node, Fragment>> edges = new HashMap<>();

            final Set<Node> highPriorityStartingNodeSet = new HashSet<>();
            final Set<Node> lowPriorityStartingNodeSet = new HashSet<>();
            final Set<Fragment> edgeFragmentSet = new HashSet<>();

            fragmentSet.forEach(fragment -> {
                if (fragment.end() != null) {
                    edgeFragmentSet.add(fragment);
                    // when we are have more info from the cache, e.g. instance count,
                    // we can have a better estimate on the cost of the fragment
                    updateFragmentCost(allNodes, nodesWithFixedCost, fragment);

                } else if (fragment.hasFixedFragmentCost()) {
                    Node node = Node.addIfAbsent(NodeId.NodeType.VAR, fragment.start(), allNodes);
                    if (fragment instanceof LabelFragment) {
                        Type type = tx.getType(Iterators.getOnlyElement(((LabelFragment) fragment).labels().iterator()));
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
                }
            });

            // convert fragments to a connected graph
            Set<Weighted<DirectedEdge<Node>>> weightedGraph = buildWeightedGraph(
                    allNodes, connectedNodes, edges, edgeFragmentSet);

            if (!weightedGraph.isEmpty()) {
                // sparse graph for better performance
                SparseWeightedGraph<Node> sparseWeightedGraph = SparseWeightedGraph.from(weightedGraph);

                // selecting starting points
                Collection<Node> startingNodes;
                if (!highPriorityStartingNodeSet.isEmpty()) {
                    startingNodes = highPriorityStartingNodeSet;
                } else if (!lowPriorityStartingNodeSet.isEmpty()) {
                    startingNodes = lowPriorityStartingNodeSet;
                } else {
                    // if all else fails, use any valid nodes
                    startingNodes = sparseWeightedGraph.getNodes().stream()
                            .filter(Node::isValidStartingPoint).collect(Collectors.toSet());
                }

                // find the minimum spanning tree for each root
                // then get the tree with minimum weight
                Arborescence<Node> arborescence = startingNodes.stream()
                        .map(node -> ChuLiuEdmonds.getMaxArborescence(sparseWeightedGraph, node))
                        .max(Comparator.comparingDouble(tree -> tree.weight))
                        .map(arborescenceInside -> arborescenceInside.val).orElse(Arborescence.empty());
                greedyTraversal(plan, arborescence, allNodes, edges);
            }
            addUnvisitedNodeFragments(plan, allNodes, connectedNodes);
        });

        // add disconnected fragment set with no edge fragment
        addUnvisitedNodeFragments(plan, allNodes, allNodes.values());
        LOG.trace("Greedy Plan = " + plan);
        return plan;
    }

    // infer type of relationship type if we know the type of the role players
    // add label fragment and isa fragment if we can infer any
    private static void inferRelationshipTypes(TransactionOLTP tx, Set<Fragment> allFragments) {

        Map<Variable, Type> labelVarTypeMap = getLabelVarTypeMap(tx, allFragments);
        if (labelVarTypeMap.isEmpty()) return;

        Multimap<Variable, Type> instanceVarTypeMap = getInstanceVarTypeMap(allFragments, labelVarTypeMap);

        Multimap<Variable, Variable> relationshipRolePlayerMap = getRelationshipRolePlayerMap(allFragments, instanceVarTypeMap);
        if (relationshipRolePlayerMap.isEmpty()) return;

        // for each type, get all possible relationship type it could be in
        Multimap<Type, RelationType> relationshipMap = HashMultimap.create();
        labelVarTypeMap.values().stream().distinct().forEach(
                type -> addAllPossibleRelationships(relationshipMap, type));

        // inferred labels should be kept separately, even if they are already in allFragments set
        Map<Label, Statement> inferredLabels = new HashMap<>();
        relationshipRolePlayerMap.asMap().forEach((relationshipVar, rolePlayerVars) -> {

            Set<Type> possibleRelationshipTypes = rolePlayerVars.stream()
                    .filter(instanceVarTypeMap::containsKey)
                    .map(rolePlayer -> getAllPossibleRelationshipTypes(
                            instanceVarTypeMap.get(rolePlayer), relationshipMap))
                    .reduce(Sets::intersection).orElse(Collections.emptySet());

            //TODO: if possibleRelationshipTypes here is empty, the query will not match any data
            if (possibleRelationshipTypes.size() == 1) {

                Type relationshipType = possibleRelationshipTypes.iterator().next();
                Label label = relationshipType.label();

                // add label fragment if this label has not been inferred
                if (!inferredLabels.containsKey(label)) {
                    Statement labelVar = var();
                    inferredLabels.put(label, labelVar);
                    Fragment labelFragment = Fragments.label(new TypeProperty(label.getValue()), labelVar.var(), ImmutableSet.of(label));
                    allFragments.add(labelFragment);
                }

                // finally, add inferred isa fragments
                Statement labelVar = inferredLabels.get(label);
                IsaProperty isaProperty = new IsaProperty(labelVar);
                EquivalentFragmentSet isaEquivalentFragmentSet = EquivalentFragmentSets.isa(isaProperty,
                        relationshipVar, labelVar.var(), relationshipType.isImplicit());
                allFragments.addAll(isaEquivalentFragmentSet.fragments());
            }
        });
    }

    private static Multimap<Variable, Variable> getRelationshipRolePlayerMap(
            Set<Fragment> allFragments, Multimap<Variable, Type> instanceVarTypeMap) {
        // relationship vars and its role player vars
        Multimap<Variable, Variable> relationshipRolePlayerMap = HashMultimap.create();
        allFragments.stream().filter(OutRolePlayerFragment.class::isInstance)
                .forEach(fragment -> relationshipRolePlayerMap.put(fragment.start(), fragment.end()));

        // find all the relationships requiring type inference
        Iterator<Variable> iterator = relationshipRolePlayerMap.keySet().iterator();
        while (iterator.hasNext()) {
            Variable relationship = iterator.next();

            // the relation should have at least 2 known role players so we can infer something useful
            if (instanceVarTypeMap.containsKey(relationship) ||
                    relationshipRolePlayerMap.get(relationship).size() < 2) {
                iterator.remove();
            } else {
                int numRolePlayersHaveType = 0;
                for (Variable rolePlayer : relationshipRolePlayerMap.get(relationship)) {
                    if (instanceVarTypeMap.containsKey(rolePlayer)) {
                        numRolePlayersHaveType++;
                    }
                }
                if (numRolePlayersHaveType < 2) {
                    iterator.remove();
                }
            }
        }
        return relationshipRolePlayerMap;
    }

    // find all vars with direct or indirect out isa edges
    private static Multimap<Variable, Type> getInstanceVarTypeMap(
            Set<Fragment> allFragments, Map<Variable, Type> labelVarTypeMap) {
        Multimap<Variable, Type> instanceVarTypeMap = HashMultimap.create();
        int oldSize;
        do {
            oldSize = instanceVarTypeMap.size();
            allFragments.stream()
                    .filter(fragment -> labelVarTypeMap.containsKey(fragment.start()))
                    .filter(fragment -> fragment instanceof InIsaFragment || fragment instanceof InSubFragment)
                    .forEach(fragment -> instanceVarTypeMap.put(fragment.end(), labelVarTypeMap.get(fragment.start())));
        } while (oldSize != instanceVarTypeMap.size());
        return instanceVarTypeMap;
    }

    // find all vars representing types
    private static Map<Variable, Type> getLabelVarTypeMap(TransactionOLTP tx, Set<Fragment> allFragments) {
        Map<Variable, Type> labelVarTypeMap = new HashMap<>();
        allFragments.stream()
                .filter(LabelFragment.class::isInstance)
                .forEach(fragment -> {
                    // TODO: labels() should return ONE label instead of a set
                    SchemaConcept schemaConcept = tx.getSchemaConcept(
                            Iterators.getOnlyElement(((LabelFragment) fragment).labels().iterator()));
                    if (schemaConcept != null && !schemaConcept.isRole() && !schemaConcept.isRule()) {
                        labelVarTypeMap.put(fragment.start(), schemaConcept.asType());
                    }
                });
        return labelVarTypeMap;
    }

    private static Set<Type> getAllPossibleRelationshipTypes(
            Collection<Type> instanceVarTypes, Multimap<Type, RelationType> relationshipMap) {

        return instanceVarTypes.stream()
                .map(rolePlayerType -> (Set<Type>) new HashSet<Type>(relationshipMap.get(rolePlayerType)))
                .reduce(Sets::intersection).orElse(Collections.emptySet());
    }

    private static void addAllPossibleRelationships(Multimap<Type, RelationType> relationshipMap, Type metaType) {
        metaType.subs().forEach(type -> type.playing().flatMap(Role::relationships)
                .forEach(relationshipType -> relationshipMap.put(type, relationshipType)));
    }

    // add unvisited node fragments to plan for each connected fragment set
    private static void addUnvisitedNodeFragments(List<Fragment> plan,
                                                  Map<NodeId, Node> allNodes,
                                                  Collection<Node> connectedNodes) {

        Set<Node> nodeWithFragment = connectedNodes.stream()
                // make sure the fragment either have no dependency or dependencies have been dealt with
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

    // return a collection of set, in each set, all the fragments are connected
    private static Collection<Set<Fragment>> getConnectedFragmentSets(
            List<Fragment> plan, Set<Fragment> allFragments,
            Map<NodeId, Node> allNodes, Set<Node> connectedNodes,
            Map<Node, Double> nodesWithFixedCost, TransactionOLTP tx) {

        allFragments.forEach(fragment -> {
            if (fragment.end() == null) {
                processFragmentWithFixedCost(plan, allNodes, connectedNodes, nodesWithFixedCost, tx, fragment);
            }

            if (!fragment.dependencies().isEmpty()) {
                processFragmentWithDependencies(allNodes, fragment);
            }
        });

        // process sub fragments here as we probably need to break the query tree
        processSubFragment(allNodes, nodesWithFixedCost, allFragments);

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

    private static void processFragmentWithFixedCost(List<Fragment> plan,
                                                     Map<NodeId, Node> allNodes,
                                                     Set<Node> connectedNodes,
                                                     Map<Node, Double> nodesWithFixedCost,
                                                     TransactionOLTP tx, Fragment fragment) {

        Node start = Node.addIfAbsent(NodeId.NodeType.VAR, fragment.start(), allNodes);
        connectedNodes.add(start);

        if (fragment.hasFixedFragmentCost()) {
            // fragments that should be done right away
            plan.add(fragment);
            double logInstanceCount = -1D;
            Long shardCount = fragment.getShardCount(tx);
            if (shardCount > 0) {
                logInstanceCount = Math.log(shardCount - 1D + SHARD_LOAD_FACTOR) +
                        Math.log(tx.shardingThreshold());
            }
            nodesWithFixedCost.put(start, logInstanceCount);
            start.setFixedFragmentCost(fragment.fragmentCost());

        } else if (fragment.dependencies().isEmpty()) {
            //fragments that should be done when a node has been visited
            start.getFragmentsWithoutDependency().add(fragment);
        }
    }

    private static void processFragmentWithDependencies(Map<NodeId, Node> allNodes, Fragment fragment) {
        // it's either neq or value fragment
        Node start = Node.addIfAbsent(NodeId.NodeType.VAR, fragment.start(), allNodes);
        Node other = Node.addIfAbsent(NodeId.NodeType.VAR, fragment.dependencies().iterator().next(),
                allNodes);

        start.getFragmentsWithDependency().add(fragment);
        other.getDependants().add(fragment);

        // check whether it's value fragment
        if (fragment instanceof ValueFragment) {
            // as value fragment is not symmetric, we need to add it again
            other.getFragmentsWithDependency().add(fragment);
            start.getDependants().add(fragment);
        }
    }

    // if in-sub starts from an indexed supertype, update the fragment cost of in-isa starting from the subtypes
    private static void processSubFragment(Map<NodeId, Node> allNodes,
                                           Map<Node, Double> nodesWithFixedCost,
                                           Set<Fragment> allFragments) {

        Set<Fragment> validSubFragments = allFragments.stream().filter(fragment -> {
            if (fragment instanceof InSubFragment) {
                Node superType = Node.addIfAbsent(NodeId.NodeType.VAR, fragment.start(), allNodes);
                if (nodesWithFixedCost.containsKey(superType) && nodesWithFixedCost.get(superType) > 0D) {
                    Node subType = Node.addIfAbsent(NodeId.NodeType.VAR, fragment.end(), allNodes);
                    return !nodesWithFixedCost.containsKey(subType);
                }
            }
            return false;
        }).collect(Collectors.toSet());

        if (!validSubFragments.isEmpty()) {
            validSubFragments.forEach(fragment -> {
                // TODO: should decrease the weight of sub type after each level
                nodesWithFixedCost.put(Node.addIfAbsent(NodeId.NodeType.VAR, fragment.end(), allNodes),
                        nodesWithFixedCost.get(Node.addIfAbsent(NodeId.NodeType.VAR, fragment.start(), allNodes)));
            });
            // recursively process all the sub fragments
            processSubFragment(allNodes, nodesWithFixedCost, allFragments);
        }
    }

    private static void updateFragmentCost(Map<NodeId, Node> allNodes,
                                           Map<Node, Double> nodesWithFixedCost,
                                           Fragment fragment) {

        // ideally, this is where we update fragment cost after we get more info and statistics of the graph
        // however, for now, only shard count is available, which is used to infer number of instances of a type
        if (fragment instanceof InIsaFragment) {
            Node type = Node.addIfAbsent(NodeId.NodeType.VAR, fragment.start(), allNodes);
            if (nodesWithFixedCost.containsKey(type) && nodesWithFixedCost.get(type) > 0) {
                fragment.setAccurateFragmentCost(nodesWithFixedCost.get(type));
            }
        }
    }

    private static Set<Weighted<DirectedEdge<Node>>> buildWeightedGraph(Map<NodeId, Node> allNodes,
                                                                        Set<Node> connectedNodes,
                                                                        Map<Node, Map<Node, Fragment>> edges,
                                                                        Set<Fragment> edgeFragmentSet) {

        final Set<Weighted<DirectedEdge<Node>>> weightedGraph = new HashSet<>();
        edgeFragmentSet.stream()
                .flatMap(fragment -> fragment.directedEdges(allNodes, edges).stream())
                // add each edge together with its weight
                .forEach(weightedDirectedEdge -> {
                    weightedGraph.add(weightedDirectedEdge);
                    connectedNodes.add(weightedDirectedEdge.val.destination);
                    connectedNodes.add(weightedDirectedEdge.val.source);
                });
        return weightedGraph;
    }

    // standard tree traversal from the root node
    // always visit the branch/node with smaller cost
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
        // expanding from the root until all nodes have been visited
        while (!reachableNodes.isEmpty()) {

            Node nodeWithMinCost = reachableNodes.stream().min(Comparator.comparingDouble(node ->
                    branchWeight(node, arborescence, edgesParentToChild, edgeFragmentChildToParent))).orElse(null);

            assert nodeWithMinCost != null : "reachableNodes is never empty, so there is always a minimum";

            // add edge fragment first, then node fragments
            Fragment fragment = getEdgeFragment(nodeWithMinCost, arborescence, edgeFragmentChildToParent);
            if (fragment != null) plan.add(fragment);

            addNodeFragmentToPlan(nodeWithMinCost, plan, nodes, true);

            reachableNodes.remove(nodeWithMinCost);
            if (edgesParentToChild.containsKey(nodeWithMinCost)) {
                reachableNodes.addAll(edgesParentToChild.get(nodeWithMinCost));
            }
        }
    }

    // recursively compute the weight of a branch
    private static double branchWeight(Node node, Arborescence<Node> arborescence,
                                       Map<Node, Set<Node>> edgesParentToChild,
                                       Map<Node, Map<Node, Fragment>> edgeFragmentChildToParent) {

        Double nodeWeight = node.getNodeWeight();

        if (nodeWeight == null) {
            nodeWeight = getEdgeFragmentCost(node, arborescence, edgeFragmentChildToParent) + nodeFragmentWeight(node);
            node.setNodeWeight(nodeWeight);
        }

        Double branchWeight = node.getBranchWeight();

        if (branchWeight == null) {
            final double[] weight = {nodeWeight};
            if (edgesParentToChild.containsKey(node)) {
                edgesParentToChild.get(node).forEach(child ->
                        weight[0] += branchWeight(child, arborescence, edgesParentToChild, edgeFragmentChildToParent));
            }
            branchWeight = weight[0];
            node.setBranchWeight(branchWeight);
        }

        return branchWeight;
    }

    // compute the total cost of a node
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

    // adding a node's fragments to plan, updating dependants' dependency map
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

        // telling their dependants that they have been visited
        node.getDependants().forEach(fragment -> {
            Node otherNode = nodes.get(new NodeId(NodeId.NodeType.VAR, fragment.start()));
            if (node.equals(otherNode)) {
                otherNode = nodes.get(new NodeId(NodeId.NodeType.VAR, fragment.dependencies().iterator().next()));
            }
            otherNode.getDependants().remove(fragment.getInverse());
            otherNode.getFragmentsWithDependencyVisited().add(fragment);

        });
        node.getDependants().clear();
    }

    // get edge fragment cost in order to map branch cost
    private static double getEdgeFragmentCost(Node node, Arborescence<Node> arborescence,
                                              Map<Node, Map<Node, Fragment>> edgeToFragment) {

        Fragment fragment = getEdgeFragment(node, arborescence, edgeToFragment);
        if (fragment != null) return fragment.fragmentCost();

        return 0D;
    }

    @Nullable
    private static Fragment getEdgeFragment(Node node, Arborescence<Node> arborescence,
                                                      Map<Node, Map<Node, Fragment>> edgeToFragment) {
        if (edgeToFragment.containsKey(node) &&
                edgeToFragment.get(node).containsKey(arborescence.getParents().get(node))) {
            return edgeToFragment.get(node).get(arborescence.getParents().get(node));
        }
        return null;
    }
}
