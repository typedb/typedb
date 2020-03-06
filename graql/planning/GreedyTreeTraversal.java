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

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import grakn.core.kb.graql.planning.Arborescence;
import grakn.core.kb.graql.planning.gremlin.Fragment;
import grakn.core.kb.graql.planning.spanningtree.graph.Node;
import grakn.core.kb.graql.planning.spanningtree.graph.NodeId;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static grakn.core.graql.planning.NodesUtil.nodeFragmentsWithoutDependencies;
import static grakn.core.graql.planning.NodesUtil.nodeVisitedDependenciesFragments;
import static grakn.core.graql.planning.NodesUtil.propagateLabels;

public class GreedyTreeTraversal {

    // standard tree traversal from the root node
    // always visit the branch/node with smaller cost
    static List<Fragment> greedyTraversal(Arborescence<Node> arborescence,
                                              Map<NodeId, Node> nodes,
                                              Map<Node, Map<Node, Fragment>> edgeFragmentChildToParent) {

        List<Fragment> plan = new LinkedList<>();

        Map<Node, Set<Node>> edgesParentToChild = new HashMap<>();
        arborescence.getParents().forEach((child, parent) -> {
            if (!edgesParentToChild.containsKey(parent)) {
                edgesParentToChild.put(parent, new HashSet<>());
            }
            edgesParentToChild.get(parent).add(child);
        });

        propagateLabels(edgesParentToChild);

        Node root = arborescence.getRoot();

        Set<Node> reachableNodes = Sets.newHashSet(root);
        // expanding from the root until all nodes have been visited
        while (!reachableNodes.isEmpty()) {

            Node nodeWithMinCost = reachableNodes.stream().min(Comparator.comparingDouble(node ->
                    branchWeight(node, arborescence, edgesParentToChild, edgeFragmentChildToParent))).orElse(null);

            Preconditions.checkNotNull(nodeWithMinCost, "reachableNodes is never empty, so there is always a minimum");

            // add fragments without dependencies first (eg. could be the index fragments)
            nodeFragmentsWithoutDependencies(nodeWithMinCost).forEach(fragment -> {
                if (fragment.hasFixedFragmentCost()) { plan.add(0, fragment); }
                else { plan.add(fragment); }
            });
            nodeWithMinCost.getFragmentsWithoutDependency().clear();

            // add edge fragment first
            Fragment fragment = getEdgeFragment(nodeWithMinCost, arborescence, edgeFragmentChildToParent);
            if (fragment != null) plan.add(fragment);

            // add node's dependant fragments
            nodeVisitedDependenciesFragments(nodeWithMinCost, nodes).forEach(frag -> {
                if (frag.hasFixedFragmentCost()) { plan.add(0, frag); }
                else { plan.add(frag); }
            });

            reachableNodes.remove(nodeWithMinCost);
            if (edgesParentToChild.containsKey(nodeWithMinCost)) {
                reachableNodes.addAll(edgesParentToChild.get(nodeWithMinCost));
            }
        }

        return plan;
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
