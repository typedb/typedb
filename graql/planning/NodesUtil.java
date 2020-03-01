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
import com.google.common.collect.Iterators;
import grakn.core.common.util.Streams;
import grakn.core.graql.planning.gremlin.fragment.LabelFragment;
import grakn.core.graql.planning.gremlin.fragment.ValueFragment;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.planning.GraknQueryPlannerException;
import grakn.core.kb.graql.planning.gremlin.Fragment;
import grakn.core.kb.graql.planning.spanningtree.graph.EdgeNode;
import grakn.core.kb.graql.planning.spanningtree.graph.InstanceNode;
import grakn.core.kb.graql.planning.spanningtree.graph.Node;
import grakn.core.kb.graql.planning.spanningtree.graph.NodeId;
import grakn.core.kb.keyspace.KeyspaceStatistics;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;

public class NodesUtil {


    static ImmutableMap<NodeId, Node> buildNodesWithDependencies(Set<Fragment> fragments) {
        // NOTE handling building the dependencies in each connected subgraph doesn't work,
        //  because dependencies can step across disconnected fragment sets, eg  `$x; $y; $x == $y`

        // build a map to squash duplicates
        Map<NodeId, Node> nodes = new HashMap<>();

        for (Fragment fragment : fragments) {
            Set<Node> fragmentNodes = fragment.getNodes();
            for (Node node : fragmentNodes) {
                NodeId nodeId = node.getNodeId();
                nodes.merge(nodeId, node, (node1, node2) -> {

                    // keep the most specific node type
                    Node nodeToKeep;
                    if (node1.getNodeTypePriority() < node2.getNodeTypePriority()) {
                        // lower is more desirable
                        nodeToKeep = node1;
                    } else {
                        nodeToKeep = node2;
                    }

                    // if either node indicates it is NOT a valid starting point, the false starting point wins
                    boolean isValidStartingPoint = node1.isValidStartingPoint() && node2.isValidStartingPoint();
                    if (!isValidStartingPoint) {
                        nodeToKeep.setInvalidStartingPoint();
                    }
                    return nodeToKeep;
                });
            }
        }

        // convert to immutable map
        ImmutableMap<NodeId, Node> immutableNodes = ImmutableMap.copyOf(nodes);

        // build the dependencies between nodes
        buildDependenciesBetweenNodes(fragments, immutableNodes);
        return immutableNodes;
    }

    private static void buildDependenciesBetweenNodes(Set<Fragment> allFragments, Map<NodeId, Node> allNodes) {
        // build dependencies between nodes
        // TODO extract this out of the Node objects themselves, if we want to keep at all
        allFragments.forEach(fragment -> {
            if (fragment.end() == null && fragment.dependencies().isEmpty()) {
                // process fragments that have fixed cost
                Node start = allNodes.get(NodeId.of(NodeId.Type.VAR, fragment.start()));
                //fragments that should be done when a node has been visited
                start.getFragmentsWithoutDependency().add(fragment);
            }
            if (!fragment.dependencies().isEmpty()) {
                // process fragments that have ordering dependencies

                // it's either neq or value fragment
                Node start = allNodes.get(NodeId.of(NodeId.Type.VAR, fragment.start()));
                Node other = allNodes.get(NodeId.of(NodeId.Type.VAR, Iterators.getOnlyElement(fragment.dependencies().iterator())));

                start.getFragmentsWithDependency().add(fragment);
                other.getDependants().add(fragment);

                // check whether it's value fragment
                if (fragment instanceof ValueFragment) {
                    // as value fragment is not symmetric, we need to add it again
                    other.getFragmentsWithDependency().add(fragment);
                    start.getDependants().add(fragment);
                }
            }
        });
    }

    static List<Fragment> nodeFragmentsWithoutDependencies(Node node) {
        return node.getFragmentsWithoutDependency().stream()
                .sorted(Comparator.comparingDouble(Fragment::fragmentCost))
                .collect(Collectors.toList());
    }

    // convert a Node to a sub-plan, updating dependants' dependency map
    static List<Fragment> nodeVisitedDependenciesFragments(Node node, Map<NodeId, Node> nodes) {
        List<Fragment> subplan = new LinkedList<>();

        node.getFragmentsWithoutDependency().addAll(node.getFragmentsWithDependencyVisited());
        subplan.addAll(node.getFragmentsWithoutDependency().stream()
                .sorted(Comparator.comparingDouble(Fragment::fragmentCost))
                .collect(Collectors.toList()));

        node.getFragmentsWithoutDependency().clear();
        node.getFragmentsWithDependencyVisited().clear();

        // telling their dependants that they have been visited
        node.getDependants().forEach(fragment -> {
            Node otherNode = nodes.get(NodeId.of(NodeId.Type.VAR, fragment.start()));
            if (node.equals(otherNode)) {
                otherNode = nodes.get(NodeId.of(NodeId.Type.VAR, fragment.dependencies().iterator().next()));
            }
            otherNode.getDependants().remove(fragment.getInverse());
            otherNode.getFragmentsWithDependencyVisited().add(fragment);

        });
        node.getDependants().clear();

        return subplan;
    }


    /**
     * We propagate across ISA edges any labels that might be found on one side of the ISA to
     * the node on the other side of the ISA (moving the label from the label fragment to the instance node)
     * @param parentToChild a mapping that represents the Query as a Tree (after the arborescence step)
     */
    static void propagateLabels(Map<Node, Set<Node>> parentToChild) {
        /*
        1. find Edge type nodes, filter these to the set of these that are ISA edges
        2. for each ISA edge, find its parent, then children. if the parent has a LabelFragment -> to children
            if the children contains a LabelFragment -> propagate to parent
         */

        // filter to nodes representing edges in the query planner
        // then reduce to the ones that are pointing to an ISA either as parent or child (this information is stored
        // on the middle node ID type)
        Set<Node> isaEdgeNodes = parentToChild.keySet().stream()
                .filter(node -> node instanceof EdgeNode)
                .filter(node -> node.getNodeId().nodeIdType().equals(NodeId.Type.ISA))
                .collect(Collectors.toSet());

        // for each of the ISA edge nodes, find the parent and and children
        for (Node node : isaEdgeNodes) {
            Set<Node> children = parentToChild.get(node);
            // can only be one parent - it's a  tree
            Node parent = parentToChild.entrySet().stream().
                    filter(entry -> entry.getValue().contains(node))
                    .map(Map.Entry::getKey)
                    .findFirst()
                    .orElseThrow(() ->  GraknQueryPlannerException.unrootedPlanningNode(node));

            Set<Fragment> parentFragments = parent.getFragmentsWithoutDependency();
            LabelFragment parentLabelFragment = parentFragments.stream()
                    .filter(LabelFragment.class::isInstance)
                    .map(LabelFragment.class::cast)
                    .findFirst()
                    .orElse(null);

            if (parentLabelFragment != null) {
                // propagate the label to the children
                Label label = Iterators.getOnlyElement(parentLabelFragment.labels().iterator());
                children.stream()
                        .filter(childNode -> childNode instanceof InstanceNode)
                        .forEach(child -> ((InstanceNode) child)
                                .setInstanceLabel(label));
            } else {
                // find the label fragment among the children if there is one
                // then propagate the label to the parent
                LabelFragment labelFragment = children.stream()
                        .flatMap(child -> child.getFragmentsWithoutDependency().stream())
                        .filter(LabelFragment.class::isInstance)
                        .map(LabelFragment.class::cast)
                        .findFirst()
                        .orElse(null);

                if (labelFragment != null && parent instanceof InstanceNode) {
                    // it's possible to have an ISA without a label at either end, so we may not end up here
                    Label label = Iterators.getOnlyElement(labelFragment.labels().iterator());
                    ((InstanceNode) parent).setInstanceLabel(label);
                }
            }
        }
    }

    /**
     * @param label type label for which the inferred count should be estimated
     * @return estimated number of inferred instances of a given type
     */
    public static long estimateInferredTypeCount(Label label, ConceptManager conceptManager, KeyspaceStatistics keyspaceStatistics){
        //TODO find a lighter estimate/way to cache it efficiently
        SchemaConcept initialType = conceptManager.getSchemaConcept(label);
        if (initialType == null || !initialType.thenRules().findFirst().isPresent()) return 0;
        long inferredEstimate = 0;

        Set<SchemaConcept> visitedTypes = new HashSet<>();
        Stack<SchemaConcept> types = new Stack<>();
        types.push(initialType);
        while(!types.isEmpty()) {
            SchemaConcept type = types.pop();
            //estimate count by assuming connectivity determined by the least populated type
            Set<Type> dependants = type.thenRules()
                    .map(rule ->
                            rule.whenTypes()
                                    .map(t -> t.subs().max(Comparator.comparing(t2 -> keyspaceStatistics.count(conceptManager, t2.label()))))
                                    .flatMap(Streams::optionalToStream)
                                    .min(Comparator.comparing(t -> keyspaceStatistics.count(conceptManager, t.label())))
                    )
                    .flatMap(Streams::optionalToStream)
                    .collect(toSet());

            if (!visitedTypes.contains(type) && !dependants.isEmpty()){
                dependants.stream()
                        .filter(at -> !visitedTypes.contains(at))
                        .filter(at -> !types.contains(at))
                        .forEach(types::add);
                visitedTypes.add(type);
            } else {
                //if type is a leaf - update counts
                Set<? extends SchemaConcept> subs = type.subs().collect(toSet());
                for (SchemaConcept sub : subs) {
                    long labelCount = keyspaceStatistics.count(conceptManager, sub.label());
                    inferredEstimate += labelCount;
                }
            }
        }
        return inferredEstimate;
    }
}
