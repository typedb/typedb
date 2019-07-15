package grakn.core.graql.gremlin;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import grakn.core.concept.Label;
import grakn.core.graql.gremlin.fragment.Fragment;
import grakn.core.graql.gremlin.fragment.Fragments;
import grakn.core.graql.gremlin.spanningtree.graph.EdgeNode;
import grakn.core.graql.gremlin.spanningtree.graph.IdNode;
import grakn.core.graql.gremlin.spanningtree.graph.InstanceNode;
import grakn.core.graql.gremlin.spanningtree.graph.Node;
import grakn.core.graql.gremlin.spanningtree.graph.SchemaNode;
import grakn.core.graql.reasoner.utils.Pair;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.statement.Variable;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static grakn.core.graql.gremlin.NodesUtil.propagateLabels;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OptimalTreeTraversalTest {
    @Test
    public void testPropagateLabelsOverInIsa() {
        Map<Node, Set<Node>> mockParentToChildQPGraph = new HashMap<>();
        Fragment labelFragment = Fragments.label(null, new Variable("typeVar"), ImmutableSet.of(Label.of("someLabel")));
        Fragment inIsaFragment = Fragments.inIsa(null, new Variable("typeVar"), new Variable("instanceVar"), true);

        Node labelNode = labelFragment.getNodes().iterator().next();
        labelNode.getFragmentsWithoutDependency().add(labelFragment);
        Set<Node> inIsaNodes = inIsaFragment.getNodes();
        Node inIsaMiddleNode = inIsaNodes.stream()
                .filter(node -> node instanceof EdgeNode)
                .findAny()
                .get();

        assertNotNull(inIsaMiddleNode);

        InstanceNode instanceVarNode = (InstanceNode) inIsaNodes.stream()
                .filter(node -> node instanceof InstanceNode && node.getNodeId().toString().contains("instanceVar"))
                .findAny()
                .get();

        assertNotNull(instanceVarNode);

        mockParentToChildQPGraph.put(labelNode, Collections.singleton(inIsaMiddleNode));
        mockParentToChildQPGraph.put(inIsaMiddleNode, Collections.singleton(instanceVarNode));

        propagateLabels(mockParentToChildQPGraph);

        assertEquals(Label.of("someLabel"), instanceVarNode.getInstanceLabel());
    }

    private Map<Node, Set<Node>> generateMockTree(int numVertexNodes) {
        Map<Node, Set<Node>> mockParentToChild = new HashMap<>();
        Node root = Mockito.mock(InstanceNode.class);
        when(root.matchingElementsEstimate(any())).thenReturn(1L);
        mockParentToChild.put(root, new HashSet<>());
        Random random = new Random(0);

        List<Node> vertexNodes = new ArrayList<>();
        vertexNodes.add(root);

        for (int i = 1; i < numVertexNodes; i++) {
            // generate a new node that is either an instance, schema, id node
            // assign a random weight to it
            // choose a node from the existing vertex (not edge!) nodes that will be its parent
            // insert it with a new edgeNode in between

            Node newNode;
            int type = random.nextInt(5);
            if (type < 3) {
                // 3x more likely than others
                newNode = mock(InstanceNode.class);
                // between 1 and 100 instances
                int instances = 1 + random.nextInt(100);
                when(newNode.matchingElementsEstimate(any())).thenReturn((long)instances);
            } else if (type == 3) {
                newNode = mock(SchemaNode.class);
                when(newNode.matchingElementsEstimate(any())).thenReturn(1L);
            } else {
                newNode = mock(IdNode.class);
                when(newNode.matchingElementsEstimate(any())).thenReturn(1L);
            }

            int parentIndex = random.nextInt(vertexNodes.size());
            Node parent = vertexNodes.get(parentIndex);
            vertexNodes.add(newNode);

            Node edgeNode = mock(EdgeNode.class);
            when(edgeNode.matchingElementsEstimate(any())).thenReturn(1L);
            mockParentToChild.putIfAbsent(parent, new HashSet<>());
            mockParentToChild.get(parent).add(edgeNode);
            mockParentToChild.putIfAbsent(edgeNode, new HashSet<>());
            mockParentToChild.put(edgeNode, Sets.newHashSet(newNode));

        }

        return mockParentToChild;
    }

    @Test
    public void scalingOptimalRecursiveTraversal() {
        for (int i = 1; i < 30; i++) {
            Map<Node, Set<Node>> parentToChild = generateMockTree(i);

            Set<Node> children = parentToChild.values().stream().reduce((a,b) -> Sets.union(a,b).immutableCopy()).get();
            Set<Node> parents = parentToChild.keySet();
            int numNodes = Sets.union(children, parents).size();
            // root is the parent that is not also a child
            parents.removeAll(children);
            Node root = parents.iterator().next();

            OptimalTreeTraversal traversal = new OptimalTreeTraversal(mock(TransactionOLTP.class));
            Map<Set<Node>, Pair<Double, Set<Node>>> memoisedResults = new HashMap<>();
            long startTime = System.nanoTime();
            double bestCost = traversal.optimal_cost_recursive(Sets.newHashSet(root), parentToChild.get(root), memoisedResults, parentToChild);
            long endTime = System.nanoTime();

            System.out.println("Tree size: " + numNodes + ", Cost: " + bestCost + ", time: " +  + (endTime - startTime)/1000000.0 + " ms" + ", iterations: " + traversal.iterations + ", products: " + traversal.productIterations);
        }
    }

    @Test
    public void scalingOptimalStackTraversal() {
        for (int i = 1; i < 30; i++) {
            Map<Node, Set<Node>> parentToChild = generateMockTree(i);

            Set<Node> children = parentToChild.values().stream().reduce((a,b) -> Sets.union(a,b).immutableCopy()).get();
            Set<Node> parents = new HashSet<>(parentToChild.keySet());
            int numNodes = Sets.union(children, parents).size();
            // root is the parent that is not also a child
            parents.removeAll(children);
            Node root = parents.iterator().next();

            OptimalTreeTraversal traversal = new OptimalTreeTraversal(mock(TransactionOLTP.class));
//            Map<Set<Node>, Pair<Double, Set<Node>>> memoisedResults = new HashMap<>();
            Map<Integer, Pair<Double, Set<Node>>> memoisedResults = new HashMap<>();
            long startTime = System.nanoTime();
            double bestCost = traversal.optimal_cost_stack(root, memoisedResults, parentToChild);
            long endTime = System.nanoTime();

            System.out.println("Tree size: " + numNodes + ", Cost: " + bestCost + ", time: " +  + (endTime - startTime)/1000000.0 + " ms" + ", iterations: " + traversal.iterations + ", products: " + traversal.productIterations);
        }
    }

    @Test
    public void scalingOptimalBottomUpStackTraversal() {
        for (int i = 1; i < 30; i++) {
            Map<Node, Set<Node>> parentToChild = generateMockTree(i);

            Map<Node, Node> parents = new HashMap<>();
            for (Map.Entry<Node, Set<Node>> entry : parentToChild.entrySet()) {
                for (Node child : entry.getValue()) {
                    parents.put(child, entry.getKey());
                }
            }

            Set<Node> allNodes = new HashSet<>(parentToChild.keySet());
            parentToChild.values().forEach(nodeSet -> allNodes.addAll(nodeSet));

            OptimalTreeTraversal traversal = new OptimalTreeTraversal(mock(TransactionOLTP.class));
//            Map<Set<Node>, Pair<Double, Set<Node>>> memoisedResults = new HashMap<>();
            Map<Integer, Pair<Double, Set<Node>>> memoisedResults = new HashMap<>();
            long startTime = System.nanoTime();
            double bestCost = traversal.optimalCostBottomUpStack(allNodes, memoisedResults, parentToChild, parents);
            long endTime = System.nanoTime();

            System.out.println("Tree size: " + allNodes.size() + ", Cost: " + bestCost + ", time: " +  + (endTime - startTime)/1000000.0 + " ms" + ", iterations: " + traversal.iterations + ", products: " + traversal.productIterations + ", short circuits: " + traversal.shortCircuits);
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
//        mockParentToChild.put(instance2_2, Sets.newHashSet(edge3_3));
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

        OptimalTreeTraversal traversal2 = new OptimalTreeTraversal(mockTx);

        Map<Integer, Pair<Double, Set<Node>>> memoised = new HashMap<>();
        Map<Node, Node> parents = new HashMap<>();
        for (Map.Entry<Node, Set<Node>> entry : mockParentToChild.entrySet()) {
            for (Node child : entry.getValue()) {
                parents.put(child, entry.getKey());
            }
        }

        Set<Node> allNodes = parents.keySet();
        allNodes.addAll(parents.values());
        long startTime2 = System.nanoTime();
        double bestCost = traversal2.optimalCostBottomUpStack(allNodes, memoised, mockParentToChild, parents);
        long endTime2 = System.nanoTime();

        System.out.println("Tree size: " + allNodes.size() + ", Cost: " + bestCost + ", time: " +  + (endTime2 - startTime2)/1000000.0 + " ms" + ", iterations: " + traversal2.iterations + ", products: " + traversal2.productIterations);
//        System.out.println(bestCost);
//        System.out.println("Time elapsed: " + (endTime - startTime)/1000000.0 + " ms");
//        System.out.println("Iterations: " + traversal.iterations);
    }
}
