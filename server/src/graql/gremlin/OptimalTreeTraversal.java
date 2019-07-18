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

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import grakn.core.graql.gremlin.fragment.Fragment;
import grakn.core.graql.gremlin.spanningtree.Arborescence;
import grakn.core.graql.gremlin.spanningtree.graph.EdgeNode;
import grakn.core.graql.gremlin.spanningtree.graph.Node;
import grakn.core.graql.gremlin.spanningtree.graph.NodeId;
import grakn.core.graql.reasoner.utils.Pair;
import grakn.core.server.session.TransactionOLTP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import static grakn.core.graql.gremlin.NodesUtil.nodeFragmentsWithoutDependencies;
import static grakn.core.graql.gremlin.NodesUtil.nodeVisitedDependenciesFragments;
import static grakn.core.graql.gremlin.NodesUtil.propagateLabels;

public class OptimalTreeTraversal {

    private static final Logger LOG = LoggerFactory.getLogger(OptimalTreeTraversal.class);

    private final Map<Node, Node> childToParent;
    private final Map<Node, Set<Node>> parentToChild;
    private Map<Node, Map<Node, Fragment>> edgeFragmentChildToParent;
    private TransactionOLTP tx;
    private Set<Node> connectedNodes;
    private Map<NodeId, Node> allNodesById;
    private int iterations;
    private int shortCircuits;

    public OptimalTreeTraversal(TransactionOLTP tx, Set<Node> connectedNodes, Map<NodeId, Node> allNodesById, Arborescence<Node> arborescence, Map<Node, Map<Node, Fragment>> edgeFragmentChildToParent) {
        this.tx = tx;
        this.connectedNodes = connectedNodes;
        this.allNodesById = allNodesById;
        this.childToParent = arborescence.getParents();
        this.parentToChild = new HashMap<>();
        arborescence.getParents().forEach((child, parent) -> {
            if (!parentToChild.containsKey(parent)) {
                parentToChild.put(parent, new HashSet<>());
            }
            parentToChild.get(parent).add(child);
        });

        this.edgeFragmentChildToParent = edgeFragmentChildToParent;
        iterations = 0;
        shortCircuits = 0;
    }

    public List<Fragment> traverse() {

        propagateLabels(parentToChild);

        Map<NodeList, Pair<Double, NodeList>> memoisedResults = new HashMap<>();

        long startTime = System.nanoTime();
        double cost = optimalCostBottomUpStack(connectedNodes, memoisedResults, parentToChild);
        double elapsedTimeMs = (System.nanoTime() - startTime)/1000000.0;
        LOG.info("QP stage 2: took " + elapsedTimeMs + " ms (" + iterations + " iterations, " + shortCircuits + ", short circuits)  to find plan with best cost: " + cost);

        // extract optimal node ordering from memoised
        List<Node> optimalNodeOrder = extractOptimalNodeOrder(memoisedResults);

        List<Fragment> plan = orderedNodesToPlan(optimalNodeOrder);

        return plan;
    }

    private List<Fragment> orderedNodesToPlan(List<Node> optimalNodeOrder) {
        List<Fragment> plan = new ArrayList<>();

        for (Node node : optimalNodeOrder) {
            // TODO we may want to sort the order of fragments from each node based on some cost
            plan.addAll(node.getFragmentsWithoutDependency());
            node.getFragmentsWithoutDependency().clear();

            // add edge fragment if this node represents an edge fragment
            if (node instanceof EdgeNode) {
                // look up the edge fragment based on which direction the parent is (MST has chosen between In/Out already)
                plan.add(edgeFragmentChildToParent.get(node).get(childToParent.get(node)));
            }

            // add node's dependant fragments
            nodeVisitedDependenciesFragments(node, allNodesById).forEach(frag -> {
                if (frag.hasFixedFragmentCost()) { plan.add(0, frag); }
                else { plan.add(frag); }
            });

        }
        return plan;
    }


    private List<Node> extractOptimalNodeOrder(Map<NodeList, Pair<Double, NodeList>> memoisedResults) {
        List<Node> orderedNodes = new ArrayList<>();
        NodeList next = new NodeList(connectedNodes);
        while (true) {
            NodeList nextNodeList = memoisedResults.get(next).getValue();
            if (nextNodeList == null) {
                orderedNodes.add(next.get(0));
                break;
            } else {
                Node newNode = Iterators.getOnlyElement(Sets.difference(new HashSet<>(next), new HashSet<>(nextNodeList)).iterator());
                orderedNodes.add(newNode);
                next = nextNodeList;
            }
        }
        // we extract the nodes in reverse order, ending with the root, so flip the list
        Collections.reverse(orderedNodes);
        return orderedNodes;
    }


    public double optimalCostBottomUpStack(Set<Node> allNodes,
                                           Map<NodeList, Pair<Double, NodeList>> memoised,
                                           Map<Node, Set<Node>> edgesParentToChild) {

        Stack<StackEntry> stack = new Stack<>();

        // find all leaves as the starting removable node set
        NodeList leafNodes = leaves(childToParent);
        stack.push(new StackEntry(new NodeList(allNodes), leafNodes));

        // we can prune the search space by stopping the search when the current cost exceeds the best cost we have found yet
        double bestCostFound = Double.MAX_VALUE; // best tree traversal cost found yet
        double currentCost = 0.0;                // partial cost of the tree traversal we are calculating

        while (stack.size() != 0) {
            iterations++;
            StackEntry entry = stack.peek();

            // if this set of nodes has been visited once already, we are in the stack size reduction
            // phase, where entries are being popped off. This also means we have guaranteed all the
            // children of this stack entry have been calculated, we can calculate the optimal
            // cost and path to take from this `visited` node set.
            if (entry.haveVisited) {

                double cost = entry.visitedNodeListCost;

                NodeList bestChild = findBestChild(entry, memoised);

                // memoise the cost of the cost of the entry.visited set plus traversing via the chosen next child set
                double bestChildCost;
                if (bestChild == null) {
                    if (entry.visited.size() == 1) {
                        bestChildCost = 0.0;
                    } else {
                        bestChildCost = Double.MAX_VALUE;
                    }
                } else {
                    bestChildCost = memoised.get(bestChild).getKey();
                }
//                double bestChildCost = bestChild == null ? 0 : memoised.get(bestChild).getKey();
                memoised.put(entry.visited, new Pair<>(cost + bestChildCost, bestChild));

                // the current cost to completion -- cost from start of stack up to a final value
                // is currentCost + bestChildCost
                double currentCostToCompletion = currentCost + bestChildCost;

                // update the global best cost found yet if we are the top of the stack and have no more paths to explore
                if (currentCostToCompletion < bestCostFound) {
                    bestCostFound = currentCostToCompletion;
                }

                // actually remove this stack entry when we see it the second time
                stack.pop();
                // remove the cost from the current total as we have finished processing this stack entry
                currentCost -= cost;

            } else {
                // compute the cost of the current visited set of nodes
                double cost = product(entry.visited); // cost of enumerating all n-tuples for the currently visited nodes

                // if this stack entry has not been visited, before we expand all of its children onto the stack
                // and mark this entry as visited. So when we finish processing its children, we are guaranteed to
                // be able to evaluate this entry

                // set that we have visited the entry for the first time
                entry.setHaveVisited();
                entry.setVisitedNodeListCost(cost);

                // include the cost of taking this branch in the best current cost
                // so the current cost includes this branch
                currentCost += cost;

                if (currentCost >= bestCostFound) {
                    // we can short circuit the execution if we are on a more expensive branch than the best yet
                    shortCircuits++;
                    stack.pop();
                    currentCost -= cost;
                    continue;
                }

                // small optimisation: when visited has size 1, we can skip because we are going
                // to remove the last element, resulting in a 0-element next visited set
                // which we can safely skip some extra computation
                if (entry.visited.size() == 1) {
                    continue;
                }

                entry.removable.sort(Comparator.comparing(node -> node.matchingElementsEstimate(tx)));

                for (Node nextNode : entry.removable) {

                    NodeList nextVisited = copyExceptElement(entry.visited, nextNode);

                    // record that this child is a dependant of the currently stack entry
                    entry.addChild(nextVisited);

                    NodeList nextRemovable = copyExceptElement(entry.removable, nextNode);
                    Node parent = childToParent.get(nextNode);
                    if (parent != null) {
                        Set<Node> siblings = edgesParentToChild.get(parent);
                        // if none of the siblings are in entry.removable, we can add the parent to removable
                        // avoiding the set intersection by checking size first can save 20% of time spent in this check
                        if (siblings.size() == 1 || isEmptyIntersection(siblings, nextVisited)) {
                            nextRemovable.add(parent);
                        }
                    }

                    // compute child if we don't already have the result for the child
                    boolean isComputed = memoised.containsKey(nextVisited);
                    if (!isComputed) {
                        stack.push(new StackEntry(nextVisited, nextRemovable));
                    }
                }
            }
        }

        double finalCost = memoised.get(new NodeList(allNodes)).getKey();
        // these assertions fail because we start adding very small numbers (eg. 1) to very big doubles (eg. 3.11111E200)
//        assert currentCost == 0;
//        assert finalCost == bestCostFound;
        return finalCost;
    }


    private NodeList findBestChild(StackEntry entry, Map<NodeList, Pair<Double, NodeList>> memoised) {
        // find the best child to choose after the current entry -
        // each child has either been calculated or skipped for having too high a cost (pruned out)
        NodeList bestChild = null;
        double bestChildCost = Double.MAX_VALUE;
        // update the cost to include the path from the best to child to the finish
        // if there are any children
        for (NodeList child : entry.children) {
            Pair<Double, NodeList> memoisedResult = memoised.get(child);
            if (memoisedResult != null && (bestChild == null || memoisedResult.getKey() < bestChildCost)) {
                bestChild = child;
                bestChildCost = memoisedResult.getKey();
            }
        }
        return bestChild;
    }

    private NodeList copyExceptElement(NodeList list, Node notToInclude) {
        NodeList copy = new NodeList();
        for (Node node : list) {
            if (node != notToInclude) {
                copy.add(node);
            }
        }
        return copy;
    }

    private boolean isEmptyIntersection(Set<Node> visited, NodeList siblings) {
        for (Node node : siblings) {
            if (visited.contains(node)) {
                return false;
            }
        }
        return true;
    }

    private double product(Collection<Node> nodes) {
        if (nodes.isEmpty()) {
            return 0.0;
        }

        double cost = 1.0;
        for (Node node : nodes) {
            cost = cost * node.matchingElementsEstimate(tx);
        }
        return cost;
    }

    private NodeList leaves(Map<Node, Node> parents) {
        Set<Node> keys = new HashSet<>(parents.keySet());
        keys.removeAll(parents.values());
        return new NodeList(keys);
    }

    class StackEntry {
        boolean haveVisited;
        NodeList visited;
        NodeList removable;
        List<NodeList> children;
        double visitedNodeListCost = -1; // cached product to save half the evaluations of product

        StackEntry(NodeList visited, NodeList removable) {
            this.visited = visited;
            this.removable = removable;
            haveVisited = false;
            children = new ArrayList<>();
        }

        void addChild(NodeList child) {
            this.children.add(child);
        }

        void setHaveVisited() {
            haveVisited = true;
        }

        void setVisitedNodeListCost(double product) {
            visitedNodeListCost = product;
        }

    }

    /*
    A semi-dangerous extension of List that caches hash codes
    When using, ensure that the values never change after the first call of hash codes!
    // TODO only make this available via a constructor function and make the implementation immutable
    Provides a 25-50% speedup of calls to memoised map, which has costs dominated by hashCode()
     */
    class NodeList extends ArrayList<Node> {
        int cachedHashCode = 0;

        NodeList(Collection<Node> nodes) {
            super(nodes);
        }

        NodeList() {
            super();
        }

        @Override
        public int hashCode() {
            if (cachedHashCode == 0) {
                cachedHashCode = super.hashCode();
            }
            return cachedHashCode;
        }
    }



}

