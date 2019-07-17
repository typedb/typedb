package grakn.core.graql.gremlin;

import com.google.common.collect.Sets;
import grakn.core.graql.gremlin.spanningtree.graph.EdgeNode;
import grakn.core.graql.gremlin.spanningtree.graph.IdNode;
import grakn.core.graql.gremlin.spanningtree.graph.InstanceNode;
import grakn.core.graql.gremlin.spanningtree.graph.Node;
import grakn.core.graql.gremlin.spanningtree.graph.NodeId;
import grakn.core.graql.reasoner.utils.Pair;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.statement.Variable;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OptimalTreeTraversalTest {

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
//        Random random = new Random(-192934);
//        Random random = new Random(3);
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
                // emulating either an IdNode or SchemaNode, occurs 2/5 of time
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


    @Test
    public void scalingOptimalBottomUpStackTraversal() {
        for (int i = 1; i < 500; i++) {
//        int seed = -182953;
//        int i = 10;

            Map<Node, Set<Node>> parentToChild = generateMockTree(i, 0);

            Map<Node, Node> parents = new HashMap<>();
            for (Map.Entry<Node, Set<Node>> entry : parentToChild.entrySet()) {
                for (Node child : entry.getValue()) {
                    parents.put(child, entry.getKey());
                }
            }

            Set<Node> allNodes = new HashSet<>(parentToChild.keySet());
            parentToChild.values().forEach(allNodes::addAll);

//            OptimalTreeTraversal traversal = new OptimalTreeTraversal(mock(TransactionOLTP.class));
//            Map<OptimalTreeTraversal.NodeList, Pair<Double, OptimalTreeTraversal.NodeList>> memoisedResults = new HashMap<>();
//            long startTime = System.nanoTime();
//            double bestCost = traversal.optimalCostBottomUpStack(allNodes, memoisedResults, parentToChild, parents);
//            long endTime = System.nanoTime();
//
//            System.out.println("Tree size: " + allNodes.size() + ", Cost: " + bestCost + ", time: " + +(endTime - startTime) / 1000000.0 + " ms" + ", iterations: " + traversal.iterations + ", products: " + traversal.productIterations + ", short circuits: " + traversal.shortCircuits);
        }
    }

    @Test
    public void testOptimalTraversalGreedyFails() {
        Map<Node, Set<Node>> mockParentToChild = new HashMap<>();
        TransactionOLTP mockTx = Mockito.mock(TransactionOLTP.class);

        Node root = Mockito.mock(InstanceNode.class);
        when(root.matchingElementsEstimate(any())).thenReturn(1L);

        Node edge1_1 = Mockito.mock(EdgeNode.class);
        when(edge1_1.matchingElementsEstimate(any())).thenReturn(1L);
        Node edge1_2 = Mockito.mock(EdgeNode.class);
        when(edge1_2.matchingElementsEstimate(any())).thenReturn(1L);
//        Node edge1_3 = Mockito.mock(EdgeNode.class);
//        when(edge1_3.matchingElementsEstimate(any())).thenReturn(1L);


        Node instance2_1 = Mockito.mock(InstanceNode.class);
        when(instance2_1.matchingElementsEstimate(any())).thenReturn(10L);
        Node instance2_2 = Mockito.mock(InstanceNode.class);
        when(instance2_2.matchingElementsEstimate(any())).thenReturn(8L);
//        Node instance2_3 = Mockito.mock(InstanceNode.class);
//        when(instance2_3.matchingElementsEstimate(any())).thenReturn(3L);

//        Node edge3_1 = Mockito.mock(EdgeNode.class);
//        when(edge3_1.matchingElementsEstimate(any())).thenReturn(1L);
//        Node edge3_2 = Mockito.mock(EdgeNode.class);
//        when(edge3_2.matchingElementsEstimate(any())).thenReturn(1L);
//        Node edge3_3 = Mockito.mock(EdgeNode.class);
//        when(edge3_3.matchingElementsEstimate(any())).thenReturn(1L);
//        Node edge3_4 = Mockito.mock(EdgeNode.class);
//        when(edge3_4.matchingElementsEstimate(any())).thenReturn(1L);

//        Node instance4_1 = Mockito.mock(SchemaNode.class);
//        when(instance4_1.matchingElementsEstimate(any())).thenReturn(1L);
//        Node instance4_2 = Mockito.mock(InstanceNode.class);
//        when(instance4_2.matchingElementsEstimate(any())).thenReturn(5L);
//        Node instance4_3 = Mockito.mock(InstanceNode.class);
//        when(instance4_3.matchingElementsEstimate(any())).thenReturn(10L);
//        Node instance4_4 = Mockito.mock(InstanceNode.class);
//        when(instance4_4.matchingElementsEstimate(any())).thenReturn(100L);

        mockParentToChild.put(root, Sets.newHashSet(edge1_1, edge1_2));//, edge1_3));
        mockParentToChild.put(edge1_1, Sets.newHashSet(instance2_1));
        mockParentToChild.put(edge1_2, Sets.newHashSet(instance2_2));
//        mockParentToChild.put(edge1_3, Sets.newHashSet(instance2_3));

//        mockParentToChild.put(instance2_1, Sets.newHashSet(edge3_1, edge3_2));
//        mockParentToChild.put(nstance2_2, Sets.newHashSet(edge3_3));
//        mockParentToChild.put(instance2_3, Sets.newHashSet(edge3_4));
//
//        mockParentToChild.put(edge3_1, Sets.newHashSet(instance4_1));
//        mockParentToChild.put(edge3_2, Sets.newHashSet(instance4_2));
//        mockParentToChild.put(edge3_3, Sets.newHashSet(instance4_3));
//        mockParentToChild.put(edge3_4, Sets.newHashSet(instance4_4));


//        OptimalTreeTraversal traversal = new OptimalTreeTraversal(mockTx);
//
//        Map<Set<Node>, Pair<Double, Set<Node>>> memoisedResults = new HashMap<>();
//        long startTime = System.nanoTime();
//        double bestCost = traversal.optimal_cost_recursive(Sets.newHashSet(root), mockParentToChild.get(root), memoisedResults, mockParentToChild);
//        long endTime = System.nanoTime();

//        OptimalTreeTraversal traversal2 = new OptimalTreeTraversal(mockTx);
//        Map<Integer, Pair<Double, Set<Node>>> memoised = new HashMap<>();
//        Map<Node, Node> parents = new HashMap<>();
//        for (Map.Entry<Node, Set<Node>> entry : mockParentToChild.entrySet()) {
//            for (Node child : entry.getValue()) {
//                parents.put(child, entry.getKey());
//            }
//        }

//        Set<Node> allNodes = parents.keySet();
//        allNodes.addAll(parents.values());
//        long startTime2 = System.nanoTime();
//        double bestCost = traversal2.optimalCostBottomUpStack(allNodes, memoised, mockParentToChild, parents);
//        long endTime2 = System.nanoTime();

//        System.out.println("Tree size: " + allNodes.size() + ", Cost: " + bestCost + ", time: " +  + (endTime2 - startTime2)/1000000.0 + " ms" + ", iterations: " + traversal2.iterations + ", products: " + traversal2.productIterations);

//        System.out.println(bestCost);
//        System.out.println("Time elapsed: " + (endTime - startTime)/1000000.0 + " ms");
//        System.out.println("Iterations: " + traversal.iterations);
    }
}
