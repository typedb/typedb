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

import com.google.common.collect.Sets;
import grakn.core.graql.gremlin.fragment.Fragment;
import grakn.core.graql.gremlin.spanningtree.Arborescence;
import grakn.core.graql.gremlin.spanningtree.graph.Node;
import grakn.core.graql.gremlin.spanningtree.graph.NodeId;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import static grakn.core.graql.gremlin.NodesUtil.nodeVisitedDependenciesFragments;
import static grakn.core.graql.gremlin.NodesUtil.nodeFragmentsWithoutDependencies;

public class DepthFirstTreeTraversal {

    static List<Fragment> dfsTraversal(Arborescence<Node> arborescence,
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

        Node root = arborescence.getRoot();

        Stack<Node> nodeStack = new Stack<>();
        nodeStack.add(root);
        // expanding from the root until all nodes have been visited
        while (!nodeStack.isEmpty()) {

            // remove the last item off the stack
            Node nextNode = nodeStack.pop();

            // add fragments without dependencies first (eg. could be the index fragments)
            nodeFragmentsWithoutDependencies(nextNode).forEach(fragment -> {
                if (fragment.hasFixedFragmentCost()) { plan.add(0, fragment); }
                else { plan.add(fragment); }
            });
            nextNode.getFragmentsWithoutDependency().clear();

            // add edge fragment first
            Fragment fragment = getEdgeFragment(nextNode, arborescence, edgeFragmentChildToParent);
            if (fragment != null) plan.add(fragment);

            // add node's dependant fragments
            nodeVisitedDependenciesFragments(nextNode, nodes).forEach(frag -> {
                if (frag.hasFixedFragmentCost()) { plan.add(0, frag); }
                else { plan.add(frag); }
            });

            if (edgesParentToChild.containsKey(nextNode)) {
                nodeStack.addAll(edgesParentToChild.get(nextNode));
            }
        }

        return plan;

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
