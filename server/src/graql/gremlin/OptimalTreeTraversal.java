package grakn.core.graql.gremlin;

import com.google.common.collect.Sets;
import grakn.core.api.Transaction;
import grakn.core.graql.gremlin.fragment.Fragment;
import grakn.core.graql.gremlin.spanningtree.Arborescence;
import grakn.core.graql.gremlin.spanningtree.graph.Node;
import grakn.core.graql.gremlin.spanningtree.graph.NodeId;
import grakn.core.graql.reasoner.utils.Pair;
import grakn.core.server.session.TransactionOLTP;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

import static grakn.core.graql.gremlin.NodesUtil.propagateLabels;

public class OptimalTreeTraversal {

    private TransactionOLTP tx;
    int iterations;
    int productIterations;
    int shortCircuits;

    Random testingRandom;

    public OptimalTreeTraversal(TransactionOLTP tx) {

        this.tx = tx;
        iterations = 0;
        productIterations = 0;
        shortCircuits = 0;

        this.testingRandom = new Random(0);
    }

    public Map<Set<Node>, Pair<Double, Set<Node>>> traverse(Arborescence<Node> arborescence,
                                                            Map<NodeId, Node> nodes,
                                                            Map<Node, Map<Node, Fragment>> edgeFragmentChildToParent) {

        List<Fragment> plan = new ArrayList<>();

        Node root = arborescence.getRoot();

        Map<Node, Set<Node>> edgesParentToChild = new HashMap<>();
        arborescence.getParents().forEach((child, parent) -> {
            if (!edgesParentToChild.containsKey(parent)) {
                edgesParentToChild.put(parent, new HashSet<>());
            }
            edgesParentToChild.get(parent).add(child);
        });

        propagateLabels(edgesParentToChild);

        Set<Node> visited = Sets.newHashSet(root);
        Set<Node> reachable = edgesParentToChild.get(root);
        Map<Set<Node>, Pair<Double, Set<Node>>> memoisedResults = new HashMap<>();

        optimal_cost_recursive(visited, reachable, memoisedResults, edgesParentToChild);

        return memoisedResults;
    }

    public double optimal_cost_recursive(Set<Node> visited,
                                         Set<Node> reachable,
                                         Map<Set<Node>, Pair<Double, Set<Node>>> memoised,
                                         Map<Node, Set<Node>> edgesParentToChild) {
        iterations++;
        // if we have visited this exact set before, we can return
        if (memoised.containsKey(visited)) {
            return memoised.get(visited).getKey();
        }

        double cost = product(visited);
        if (reachable.size() == 0) {
            memoised.put(visited, new Pair<>(cost, null));
            return cost;
        }

        double bestCost = 0;
        Set<Node> bestNextVisited = null;
        for (Node node : reachable) {
            // copy
            Set<Node> nextVisited = new HashSet<>(visited);
            nextVisited.add(node);

            // copy
            Set<Node> nextReachable = new HashSet<>(reachable);
            if (edgesParentToChild.containsKey(node)) {
                nextReachable.addAll(edgesParentToChild.get(node));
            }
            nextReachable.remove(node);

            double nextCost = cost + optimal_cost_recursive(nextVisited, nextReachable, memoised, edgesParentToChild);

            if (bestNextVisited == null || nextCost < bestCost) {
                bestCost = nextCost;
                bestNextVisited = nextVisited;
            }
        }

        // update memoised result
        memoised.put(visited, new Pair<>(bestCost, bestNextVisited));

        return bestCost;
    }

    class StackEntry {
        boolean haveVisited;
        Set<Node> visited;
        Set<Node> reachable;
        List<Set<Node>> children;

        public StackEntry(Set<Node> visited, Set<Node> reachable) {
            this.visited = visited;
            this.reachable = reachable;
            haveVisited = false;
            children = new ArrayList<>();
        }

        public void addChild(Set<Node> child) {
            this.children.add(child);
        }

        public void setHaveVisited() {
            haveVisited = true;
        }
    }

    public double optimal_cost_stack(Node root,
//                                     Map<Set<Node>, Pair<Double, Set<Node>>> memoised,
                                     Map<Integer, Pair<Double, Set<Node>>> memoised,
                                     Map<Node, Set<Node>> edgesParentToChild) {

        Set<Node> visited = Sets.newHashSet(root);
        Set<Node> reachable = edgesParentToChild.get(root);

        Stack<StackEntry> stack = new Stack<>();
        stack.push(new StackEntry(visited, reachable));

        long timeSpentInMemoised = 0l;
        long timeSpentInElse = 0l;
        long timeSpentInProduct = 0l;
        long start1;
        long start2;
        long start3;


//        double currentCost = 0.0;
//        double bestFoundCost = Double.MAX_VALUE;

        while (stack.size() != 0) {
            iterations++;
            StackEntry entry = stack.peek();

            // compute the cost of the current visited set
            start3 = System.nanoTime();
            double cost = product(entry.visited);
            timeSpentInProduct += (System.nanoTime() - start3);

            if (entry.haveVisited) {
                // actually remove this stack entry when we see it the second time
                stack.pop();

                // find the best child to choose next
                Set<Node> bestChild = null;
                double bestChildCost = 0.0;
                if (entry.children.size() > 0) {
                    // update the cost to include the path from the best to child to the finish
                    // if there are any children
                    for (Set<Node> child : entry.children) {
                        start1 = System.nanoTime();
                        Pair<Double, Set<Node>> memoisedResult = memoised.get(child.hashCode());
                        timeSpentInMemoised += (System.nanoTime() - start1);
                        if (memoisedResult != null && (bestChild == null || memoisedResult.getKey() < bestChildCost)) {
                            bestChild = child;
                            bestChildCost = memoisedResult.getKey();
                        }
                    }
                }

                // memoise the cost
                start1 = System.nanoTime();
                memoised.put(entry.visited.hashCode(), new Pair<>(cost + bestChildCost, bestChild));
                timeSpentInMemoised += (System.nanoTime() - start1);

//                // update the global best cost found yet if we are the top of the stack and have no more paths to explore
//                if (entry.children.size() == 0 && currentCost < bestFoundCost) {
//                    bestFoundCost = currentCost;
//                }

//                 remove the cost from the current total as we have finished processing this stack entry
//                currentCost -= cost;

            } else {
                start2 = System.nanoTime();
                // set that we have visited the entry for the first time

//                // this branch means we are expanding this path of exploration
//                // so the current cost includes this branch
//                currentCost += cost;

                entry.setHaveVisited();

//                // This never short circuits because the last term of the cost always dominates
//                if (currentCost > bestFoundCost) {
//                    // we can short circuit the execution if we are on a more expensive branch than the best yet
//                    shortCircuits++;
//                    stack.pop();
//                    currentCost -= cost;
//                    continue;
//                }

                for (Node nextNode : entry.reachable) {
                    Set<Node> nextVisited = new HashSet<>(entry.visited);
                    nextVisited.add(nextNode);

                    // record that this child is a dependant of the currently stack entry
                    entry.addChild(nextVisited);

                    // compute child if we don't already have the result for the child
                    start1 = System.nanoTime();
                    boolean isComputed = memoised.containsKey(nextVisited.hashCode());
                    timeSpentInMemoised += (System.nanoTime() - start1);
                    if (!isComputed) {
                        Set<Node> nextReachable = new HashSet<>(entry.reachable);
                        if (edgesParentToChild.get(nextNode) != null) {
                            nextReachable.addAll(edgesParentToChild.get(nextNode));
                        }
                        nextReachable.remove(nextNode);
                        stack.add(new StackEntry(nextVisited, nextReachable));
                    }
                }
                timeSpentInElse += (System.nanoTime() - start2);
            }
        }


        System.out.println("Time spent in memoised: " + (timeSpentInMemoised / 1000000.0));
        System.out.println("Time spent in else: " + (timeSpentInElse / 1000000.0));
        System.out.println("Time spent in product: " + (timeSpentInProduct / 1000000.0));
        return memoised.get(Sets.newHashSet(root).hashCode()).getKey();
    }

    class StackEntryBottomUp {
        boolean haveVisited;
        NodeSet visited;
        NodeSet removable;
        List<NodeSet> children;

        public StackEntryBottomUp(NodeSet visited, NodeSet removable) {
            this.visited = visited;
            this.removable = removable;
            haveVisited = false;
            children = new ArrayList<>();
        }

        public void addChild(NodeSet child) {
            this.children.add(child);
        }

        public void setHaveVisited() {
            haveVisited = true;
        }
    }

    /*
    A semi-dangerous extension of HashSet that caches hash codes
    When using, ensure that the values never change after the first call of hash codes!
    // TODO only make this available via a constructor function and make the set implementation immutable
    Provides a 25-50% speedup of calls to memoised map, which is dominated by hashCode()
     */
    class NodeSet extends HashSet<Node> {
        int cachedHashCode = 0;

        public NodeSet(Set<Node> nodes) {
            super(nodes);
        }

        public NodeSet(Collection<Node> nodes) {
            super(nodes);
        }

        @Override
        public int hashCode() {
            if (cachedHashCode == 0) {
                cachedHashCode = super.hashCode();
            }
            return cachedHashCode;
        }
    }

    public double optimalCostBottomUpStack(Set<Node> allNodes,
                                           Map<Integer, Pair<Double, NodeSet>> memoised,
                                           Map<Node, Set<Node>> edgesParentToChild,
                                           Map<Node, Node> parents) {

        Stack<StackEntryBottomUp> stack = new Stack<>();

        // find all leaves as the starting removable node set
        NodeSet leaves = new NodeSet(parents.keySet());
        NodeSet allParents = new NodeSet(parents.values());
        leaves.removeAll(allParents);

        stack.push(new StackEntryBottomUp(new NodeSet(allNodes), leaves));

        long timeSpentInMemoised = 0l;
        long timeSpentInElse = 0l;
        long timeSpentInProduct = 0l;
        long timeSpentInRemovable = 0l;
        long timeSpentInNextRemovable = 0l;
        long timeSpentInNextVisited = 0;
        long start1;
        long start2;
        long start3;
        long start4;
        long start5;
        long start6;


        double bestCostFound = Double.MAX_VALUE;
        double currentCost = 0.0;

        while (stack.size() != 0) {
            iterations++;
            StackEntryBottomUp entry = stack.peek();

            // compute the cost of the current visited set
            start3 = System.nanoTime();
            double cost = product(entry.visited);
            timeSpentInProduct += (System.nanoTime() - start3);


            if (entry.haveVisited) {
                // actually remove this stack entry when we see it the second time
                stack.pop();

                // find the best child to choose next
                NodeSet bestChild = null;
                double bestChildCost = 0.0;
                if (entry.children.size() > 0) {
                    // update the cost to include the path from the best to child to the finish
                    // if there are any children
                    for (NodeSet child : entry.children) {
                        start1 = System.nanoTime();
                        Pair<Double, NodeSet> memoisedResult = memoised.get(child.hashCode());
                        timeSpentInMemoised += (System.nanoTime() - start1);
                        if (memoisedResult != null && (bestChild == null || memoisedResult.getKey() < bestChildCost)) {
                            bestChild = child;
                            bestChildCost = memoisedResult.getKey();
                        }
                    }
                }

                // memoise the cost
                start1 = System.nanoTime();
                memoised.put(entry.visited.hashCode(), new Pair<>(cost + bestChildCost, bestChild));
                timeSpentInMemoised += (System.nanoTime() - start1);

//                // update the global best cost found yet if we are the top of the stack and have no more paths to explore
//                if (entry.children.size() == 0 && currentCost < bestCostFound) {
//                    bestCostFound = currentCost;
//                }
//
//                // remove the cost from the current total as we have finished processing this stack entry
//                currentCost -= cost;

            } else {
                start2 = System.nanoTime();
                // set that we have visited the entry for the first time

                // this branch means we are expanding this path of exploration
                // so the current cost includes this branch
//                currentCost += cost;

                entry.setHaveVisited();

//                if (currentCost > bestCostFound) {
//                    // we can short circuit the execution if we are on a more expensive branch than the best yet
//                    shortCircuits++;
//                    stack.pop();
//                    currentCost -= cost;
//                    continue;
//                }

                // small optimisation: when visited has size 1, we can skip because we are going
                // to remove the last element, resulting in a 0-element next visited set
                // which we can safely skip some extra computation
                if (entry.visited.size() == 1) {
                    continue;
                }


                start4 = System.nanoTime();
                List<Node> removableNodes = new ArrayList<>(entry.removable);
                removableNodes.sort(Comparator.comparing(node -> 2));//node.matchingElementsEstimate(tx)));
                timeSpentInRemovable += (System.nanoTime() - start4);

                for (Node nextNode : removableNodes) {

                    start6 = System.nanoTime();
                    NodeSet nextVisited = new NodeSet(entry.visited);
                    nextVisited.remove(nextNode);

                    // record that this child is a dependant of the currently stack entry
                    entry.addChild(nextVisited);
                    timeSpentInNextVisited += (System.nanoTime() - start6);

                    start5 = System.nanoTime();
                    NodeSet nextRemovable = new NodeSet(entry.removable);
                    nextRemovable.remove(nextNode);
                    Node parent = parents.get(nextNode);
                    if (parent != null) {
                        Set<Node> siblings = edgesParentToChild.get(parent);
                        // if none of the siblings are in entry.removable, we can add the parent to removable
                        if (Sets.intersection(nextVisited, siblings).size() == 0) {
                            nextRemovable.add(parent);
                        }
                    }
                    timeSpentInNextRemovable += (System.nanoTime() - start5);



                    // compute child if we don't already have the result for the child
                    start1 = System.nanoTime();
                    boolean isComputed = memoised.containsKey(nextVisited.hashCode());
                    timeSpentInMemoised += (System.nanoTime() - start1);
                    if (!isComputed) {
                        stack.add(new StackEntryBottomUp(nextVisited, nextRemovable));
                    }
                }
                timeSpentInElse += (System.nanoTime() - start2);
            }
        }


        System.out.println("Time spent in memoised: " + (timeSpentInMemoised / 1000000.0));
        System.out.println("Time spent in else: " + (timeSpentInElse / 1000000.0));
        System.out.println("Time spent in sortRemovable: " + (timeSpentInRemovable/1000000.0));
        System.out.println("Time spent generating nextVisited: " + (timeSpentInNextVisited/ 1000000.0));
        System.out.println("Time spent generating nextRemovable: " + (timeSpentInNextRemovable / 1000000.0));
        System.out.println("Time spent in product: " + (timeSpentInProduct / 1000000.0));
        return memoised.get(allNodes.hashCode()).getKey();
    }

    private Set<Node> removable(Set<Node> nodes, Map<Node, Set<Node>> parentToChild) {
        Set<Node> removableNodes = new HashSet<>();
        for (Node node : nodes) {
            Set<Node> children = parentToChild.get(node);
            if (children == null) {
                removableNodes.add(node);
                continue;
            }
            // if none of the children are in `nodes`, then we can remove it
            // ie. intersection(children, removableNodes) == empty set, then can remove
            Set<Node> intersection = new HashSet<>(children); // have to make a new set so we don't end up modifying parentToChild
            intersection.retainAll(nodes);
            if (intersection.size() == 0) {
                removableNodes.add(node);
            }
        }
        return removableNodes;
    }

    private double product(Set<Node> nodes) {
        productIterations++;
        double cost = 1.0;
        // this is an expensive operation - it may be the mocks in tests that are slow, as writing constants for node.matchingElementsEstimate() is much faster
        for (Node node : nodes) {
            cost = cost * 2;//node.matchingElementsEstimate(tx);
        }
        return cost;
    }
}

