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
import grakn.core.graql.gremlin.fragment.Fragment;
import grakn.core.graql.gremlin.fragment.ValueFragment;
import grakn.core.graql.gremlin.spanningtree.graph.Node;
import grakn.core.graql.gremlin.spanningtree.graph.NodeId;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
                    // key point: if any fragment indicates a node is not a valid starting point, it never is!
                    if (!node1.isValidStartingPoint()) {
                        return node1;
                    } else {
                        // either node2 is a valid starting point or it doesn't matter
                        return node2;
                    }
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
                Node start = allNodes.get(NodeId.of(NodeId.NodeType.VAR, fragment.start()));
                //fragments that should be done when a node has been visited
                start.getFragmentsWithoutDependency().add(fragment);
            }
            if (!fragment.dependencies().isEmpty()) {
                // process fragments that have ordering dependencies

                // it's either neq or value fragment
                Node start = allNodes.get(NodeId.of(NodeId.NodeType.VAR, fragment.start()));
                Node other = allNodes.get(NodeId.of(NodeId.NodeType.VAR, Iterators.getOnlyElement(fragment.dependencies().iterator())));

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
            Node otherNode = nodes.get(NodeId.of(NodeId.NodeType.VAR, fragment.start()));
            if (node.equals(otherNode)) {
                otherNode = nodes.get(NodeId.of(NodeId.NodeType.VAR, fragment.dependencies().iterator().next()));
            }
            otherNode.getDependants().remove(fragment.getInverse());
            otherNode.getFragmentsWithDependencyVisited().add(fragment);

        });
        node.getDependants().clear();

        return subplan;
    }

}
