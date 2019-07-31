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
import grakn.core.graql.gremlin.spanningtree.Arborescence;
import grakn.core.graql.gremlin.spanningtree.graph.EdgeNode;
import grakn.core.graql.gremlin.spanningtree.graph.IdNode;
import grakn.core.graql.gremlin.spanningtree.graph.InstanceNode;
import grakn.core.graql.gremlin.spanningtree.graph.Node;
import grakn.core.graql.gremlin.spanningtree.graph.NodeId;
import grakn.core.graql.reasoner.utils.Pair;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.statement.Variable;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Stress the OptimalTreeTraversal to determine implementation weak points, optimisations, and scalability of the algorithm
 */
public class OptimalTreeTraversalTest {

    /**
     * Ideally we should be able to handle up to 200 nodes (100 vertex nodes) in 100ms or something similar in this simplified environment
     * Where we don't actually have to query janus for counts on types
     */
    @Test
    public void scalingOptimalBottomUpStackTraversal() {
        for (int i = 1; i < 100; i++) {
            Map<Node, Set<Node>> parentToChild = generateMockTree(i, 0);

            Map<Node, Node> childToParent = new HashMap<>();
            for (Map.Entry<Node, Set<Node>> entry : parentToChild.entrySet()) {
                for (Node child : entry.getValue()) {
                    childToParent.put(child, entry.getKey());
                }
            }

            Set<Node> allNodes = new HashSet<>(parentToChild.keySet());
            parentToChild.values().forEach(allNodes::addAll);

            Arborescence<Node> mockArborescence = Mockito.mock(Arborescence.class);
            when(mockArborescence.getParents()).thenReturn(ImmutableMap.<Node, Node>builder().build());
            // allow up to 5 seconds to see how long processing actually is
            OptimalTreeTraversal traversal = new OptimalTreeTraversal(mock(TransactionOLTP.class), allNodes, null, mockArborescence, null, 5000);
            Map<OptimalTreeTraversal.NodeList, Pair<Double, OptimalTreeTraversal.NodeList>> memoisedResults = new HashMap<>();
            long startTime = System.nanoTime();
            double bestCost = traversal.optimalCostBottomUpStack(allNodes, childToParent, parentToChild, memoisedResults);
            long middleTime = System.nanoTime();
            List<Node> nodesPlan = traversal.extractOptimalNodeOrder(memoisedResults);
            long endTime = System.nanoTime();

            System.out.println("Tree size: " + allNodes.size() + ", Cost: " + bestCost + ", plan time: " + +(middleTime - startTime) / 1000000.0 + " ms" + ", iterations: " + traversal.iterations + ", short circuits: " + traversal.shortCircuits + ", extract time: " + (endTime - middleTime) / 1000000.0 + " ms");

            assertTrue((endTime - startTime) / 1000000.0 < 100);
        }
    }


    @Test
    public void testBottomUpStackTraversalCorrectness() {
        for (int i = 0; i < 30; i += 2) {
            Map<Node, Set<Node>> parentToChild = generateMockTree(i, 0);

            // root is node that is a parent and not also a child
            Set<Node> parents = parentToChild.keySet();
            Set<Node> children = parentToChild.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
            Node root = Iterators.getOnlyElement(Sets.difference(parents, children).iterator());

            Set<Node> visited = Sets.newHashSet(root);
            Set<Node> reachable = parentToChild.get(root);
            Map<Set<Node>, Pair<Double, Set<Node>>> memoisedResults = new HashMap<>();

            double bestCostRecursive = optimalRecursiveTopDownCost(visited, reachable, memoisedResults, parentToChild);


            Map<Node, Node> childToParent = new HashMap<>();
            for (Map.Entry<Node, Set<Node>> entry : parentToChild.entrySet()) {
                for (Node child : entry.getValue()) {
                    childToParent.put(child, entry.getKey());
                }
            }

            Set<Node> allNodes = new HashSet<>(parentToChild.keySet());
            parentToChild.values().forEach(allNodes::addAll);

            Arborescence<Node> mockArborescence = Mockito.mock(Arborescence.class);
            when(mockArborescence.getParents()).thenReturn(ImmutableMap.<Node, Node>builder().build());
            // allow up to 5 seconds to see how long processing actually is
            OptimalTreeTraversal traversal = new OptimalTreeTraversal(mock(TransactionOLTP.class), allNodes, null, mockArborescence, null, 5000);
            Map<OptimalTreeTraversal.NodeList, Pair<Double, OptimalTreeTraversal.NodeList>> memoisedResultsBottomUp = new HashMap<>();
            double bestCostBottomUp = traversal.optimalCostBottomUpStack(allNodes, childToParent, parentToChild, memoisedResultsBottomUp);

            // must agree within some multiplier of the cost due to arithmetic precision issues that become pronounced after about 16 nodes
            assertEquals(bestCostRecursive, bestCostBottomUp, 0.0000000001*bestCostBottomUp);

        }
    }



    /**
     * A mock node that isn't actually a mock for testing/optimising the operations in OptimalTreeTraversal
     * Allows us to build a realistic query tree structure with randomised values for estimates of matching elements
     */
    class TestingNode extends Node {

        private final long matchingElements;
        private final String mockType;

        public TestingNode(NodeId nodeId, long matchingElements, String mockType) {
            super(nodeId);
            this.matchingElements = matchingElements;
            this.mockType = mockType;
        }

        @Override
        public long matchingElementsEstimate(TransactionOLTP tx) {
            return matchingElements;
        }

        @Override
        public int getNodeTypePriority() {
            return 0;
        }
    }


    /**
     * Builds a fake query planner tree, following the structure of having an EdgeNode equivalent
     * between each pair of vertex nodes
     */
    private Map<Node, Set<Node>> generateMockTree(int numVertexNodes, int seed) {
        Map<Node, Set<Node>> mockParentToChild = new HashMap<>();
        NodeId rootId = NodeId.of(NodeId.Type.VAR, new Variable("root"));
        Node root = new TestingNode(rootId, 1L, InstanceNode.class.getName());
        mockParentToChild.put(root, new HashSet<>());
        Random random = new Random(seed);

        List<Node> vertexNodes = new ArrayList<>();
        vertexNodes.add(root);

        for (int i = 1; i < numVertexNodes; i++) {
            // generate a new node that is either an instance, schema, id node
            // assign a random weight to it
            // choose a node from the existing vertex (not edge!) nodes that will be its parent
            // insert it with a new edgeNode in between

            Node newNode;
            NodeId newNodeId = NodeId.of(NodeId.Type.VAR, new Variable("v" + i));
            int type = random.nextInt(5);
            if (type < 3) {
                // emulating having an InstanceNode, 3/5 of the time
                // between 1 and 100 instances
                int instances = 1 + random.nextInt(100);
                newNode = new TestingNode(newNodeId, instances, InstanceNode.class.getName());
            } else {
                // emulating either an IdNode, IndexedNode, or SchemaNode, occurs 2/5 of time
                newNode = new TestingNode(newNodeId, 1L, IdNode.class.getName());
            }

            int parentIndex = random.nextInt(vertexNodes.size());
            Node parent = vertexNodes.get(parentIndex);
            vertexNodes.add(newNode);

            // emulating an EdgeNode, always inserting an EdgeNode between VertexNode instances
            Node edgeNode = new TestingNode(NodeId.of(NodeId.Type.PLAYS, new Variable("e" + i)), 1L, EdgeNode.class.getName());
            mockParentToChild.putIfAbsent(parent, new HashSet<>());
            mockParentToChild.get(parent).add(edgeNode);
            mockParentToChild.putIfAbsent(edgeNode, new HashSet<>());
            mockParentToChild.put(edgeNode, Sets.newHashSet(newNode));
        }

        return mockParentToChild;
    }


    /**
     * Implements the optimal tree traversal as a recursive, top down algorithm
     * as opposed to a bottom up stack based algorithm.
     * Also expects the tree structure to be a [vertex node] alternating with [edge node]
     */
    private static double optimalRecursiveTopDownCost(Set<Node> visited,
                                                      Set<Node> reachable,
                                                      Map<Set<Node>, Pair<Double, Set<Node>>> memoised,
                                                      Map<Node, Set<Node>> edgesParentToChild) {
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
            // copy
            Set<Node> nextReachable = new HashSet<>(reachable);

            nextVisited.add(node);
            nextReachable.remove(node);

            // bundle EdgeNode with target node
            node = edgesParentToChild.get(node).iterator().next();
            nextVisited.add(node);

            if (edgesParentToChild.containsKey(node)) {
                nextReachable.addAll(edgesParentToChild.get(node));
            }

            double nextCost = cost + optimalRecursiveTopDownCost(nextVisited, nextReachable, memoised, edgesParentToChild);

            if (bestNextVisited == null || nextCost < bestCost) {
                bestCost = nextCost;
                bestNextVisited = nextVisited;
            }
        }

        // update memoised result
        memoised.put(visited, new Pair<>(bestCost, bestNextVisited));

        return bestCost;
    }


    private static double product(Collection<Node> nodes) {
        if (nodes.isEmpty()) {
            return 0.0;
        }

        double cost = 1.0;
        for (Node node : nodes) {
            cost = cost * node.matchingElementsEstimate(null);
        }
        return cost;
    }
}
