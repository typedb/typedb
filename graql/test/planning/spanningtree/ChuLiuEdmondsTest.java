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
 */

package grakn.core.graql.planning.spanningtree;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import grakn.core.graql.planning.spanningtree.graph.DenseWeightedGraph;
import grakn.core.kb.graql.planning.Arborescence;
import grakn.core.kb.graql.planning.ChuLiuEdmonds;
import grakn.core.kb.graql.planning.spanningtree.graph.DirectedEdge;
import grakn.core.kb.graql.planning.spanningtree.graph.InstanceNode;
import grakn.core.kb.graql.planning.spanningtree.graph.Node;
import grakn.core.kb.graql.planning.spanningtree.graph.NodeId;
import grakn.core.kb.graql.planning.spanningtree.graph.SparseWeightedGraph;
import grakn.core.kb.graql.planning.spanningtree.graph.WeightedGraph;
import grakn.core.kb.graql.planning.spanningtree.util.Weighted;
import graql.lang.statement.Variable;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static grakn.core.kb.graql.planning.spanningtree.util.Weighted.weighted;
import static org.junit.Assert.assertEquals;


public class ChuLiuEdmondsTest {
    final static double DELTA = 0.001;
    final static double NINF = Double.NEGATIVE_INFINITY;

    static Map<NodeId, Node> nodes = new HashMap<>();
    static Node node0 = new InstanceNode(NodeId.of(NodeId.Type.VAR, new Variable("0")));
    static Node node1 = new InstanceNode(NodeId.of(NodeId.Type.VAR, new Variable("1")));
    static Node node2 = new InstanceNode(NodeId.of(NodeId.Type.VAR, new Variable("2")));
    static Node node3 = new InstanceNode(NodeId.of(NodeId.Type.VAR, new Variable("3")));
    static Node node4 = new InstanceNode(NodeId.of(NodeId.Type.VAR, new Variable("4")));
    static Node node5 = new InstanceNode(NodeId.of(NodeId.Type.VAR, new Variable("5")));
    static Node node6 = new InstanceNode(NodeId.of(NodeId.Type.VAR, new Variable("6")));
    static Node node7 = new InstanceNode(NodeId.of(NodeId.Type.VAR, new Variable("7")));
    static Node node8 = new InstanceNode(NodeId.of(NodeId.Type.VAR, new Variable("8")));
    static Node node9 = new InstanceNode(NodeId.of(NodeId.Type.VAR, new Variable("9")));
    static Node node10 = new InstanceNode(NodeId.of(NodeId.Type.VAR, new Variable("10")));
    static WeightedGraph graph = SparseWeightedGraph.from(ImmutableList.of(
            weighted(DirectedEdge.from(node0).to(node1), 5),
            weighted(DirectedEdge.from(node0).to(node2), 1),
            weighted(DirectedEdge.from(node0).to(node3), 1),
            weighted(DirectedEdge.from(node1).to(node2), 11),
            weighted(DirectedEdge.from(node1).to(node3), 4),
            weighted(DirectedEdge.from(node2).to(node1), 10),
            weighted(DirectedEdge.from(node2).to(node3), 5),
            weighted(DirectedEdge.from(node3).to(node1), 9),
            weighted(DirectedEdge.from(node3).to(node2), 8)
    ));

    @BeforeClass
    public static void setupNodesMap() {
        nodes.put(node0.getNodeId(), node0);
        nodes.put(node1.getNodeId(), node1);
        nodes.put(node2.getNodeId(), node2);
        nodes.put(node3.getNodeId(), node3);
        nodes.put(node4.getNodeId(), node4);
        nodes.put(node5.getNodeId(), node5);
        nodes.put(node6.getNodeId(), node6);
        nodes.put(node7.getNodeId(), node7);
        nodes.put(node8.getNodeId(), node8);
        nodes.put(node9.getNodeId(), node9);
        nodes.put(node10.getNodeId(), node10);

    }

    static void assertEdgesSumToScore(WeightedGraph originalEdgeWeights, Weighted<Arborescence<Node>> bestTree) {
        final Map<Node, Node> parentsMap = bestTree.val.getParents();
        double sumOfWeights = 0.0;
        for (Node dest : parentsMap.keySet()) {
            final Node source = parentsMap.get(dest);
            sumOfWeights += originalEdgeWeights.getWeightOf(source, dest);
        }
        assertEquals(sumOfWeights, bestTree.weight, DELTA);
    }

    private long nodeIdVariableAsLong(Node node) {
        return Long.parseLong(node.getNodeId().getVars().iterator().next().name());
    }

    @Test
    public void testNegativeWeightWithNodeObject() {
        Node node0 = nodes.get(NodeId.of(NodeId.Type.VAR, new Variable("0")));
        Node node1 = nodes.get(NodeId.of(NodeId.Type.VAR, new Variable("1")));
        Node node2 = nodes.get(NodeId.of(NodeId.Type.VAR, new Variable("2")));
        final WeightedGraph Isa = SparseWeightedGraph.from(ImmutableList.of(
                weighted(DirectedEdge.from(node0).to(node1), -0.69),
                weighted(DirectedEdge.from(node1).to(node2), 0),
                weighted(DirectedEdge.from(node2).to(node1), -4.62),
                weighted(DirectedEdge.from(node1).to(node0), 0)
        ));
        Weighted<Arborescence<Node>> weightedSpanningTree = ChuLiuEdmonds.getMaxArborescence(Isa, node2);
        assertEquals(-4.62, weightedSpanningTree.weight, DELTA);
        ImmutableMap<Node, Node> edges = weightedSpanningTree.val.getParents();
        assertEquals(2, edges.size());
        assertEquals(node2, edges.get(node1));
        assertEquals(node1, edges.get(node0));
    }

    @Test
    public void testGetMaxSpanningTree() {
        /*
        root    10
        (0) -------> (1) \
         |  \       /  ^  \
         |   \30   |   |20 \
         |10  \    |10 |    \10
         |     \   |  /      \
         V  15  V  V /   20   V
        (3)<----- (2) -----> (4)
          \-------^
             40
         */
        double[][] weights = {
                {NINF, 10, 30, 10, NINF},
                {NINF, NINF, 10, NINF, 10},
                {NINF, 20, NINF, 15, 20},
                {NINF, NINF, 40, NINF, NINF},
                {NINF, NINF, NINF, NINF, NINF},
        };


        List<Node> nodes = Arrays.asList(node0, node1, node2, node3, node4);

        final DenseWeightedGraph graph = DenseWeightedGraph.from(nodes, weights);
        final Weighted<Arborescence<Node>> weightedSpanningTree = ChuLiuEdmonds.getMaxArborescence(graph, node0);
        /*
        root
        (0)           (1)
         |             ^
         |             |
         |             |
         |            /
         V           /
        (3)       (2) ------> (4)
          \-------^
         */
        final Map<Node, Node> maxBranching = weightedSpanningTree.val.getParents();
        assertEquals(2, nodeIdVariableAsLong(maxBranching.get(node1)));
        assertEquals(3, nodeIdVariableAsLong(maxBranching.get(node2)));
        assertEquals(0, nodeIdVariableAsLong(maxBranching.get(node3)));
        assertEquals(2, nodeIdVariableAsLong(maxBranching.get(node4)));
        assertEquals(90.0, weightedSpanningTree.weight, DELTA);
        assertEdgesSumToScore(graph, weightedSpanningTree);
    }

    @Test
    public void testRequiredAndBannedEdges() {
        final Weighted<Arborescence<Node>> weightedSpanningTree = ChuLiuEdmonds.getMaxArborescence(
                graph,
                ImmutableSet.of(DirectedEdge.from(node0).to(node1)),
                ImmutableSet.of(DirectedEdge.from(node2).to(node3)));
        final Map<Node, Node> maxBranching = weightedSpanningTree.val.getParents();
        assertEquals(0, nodeIdVariableAsLong(maxBranching.get(node1)));
        assertEquals(1, nodeIdVariableAsLong(maxBranching.get(node2)));
        assertEquals(1, nodeIdVariableAsLong(maxBranching.get(node3)));
        assertEquals(20.0, weightedSpanningTree.weight, DELTA);
        assertEdgesSumToScore(graph, weightedSpanningTree);

    }

    @Test
    public void testRequiredAndBannedEdges2() {
        final Weighted<Arborescence<Node>> weightedSpanningTree = ChuLiuEdmonds.getMaxArborescence(
                graph,
                ImmutableSet.of(DirectedEdge.from(node0).to(node3), DirectedEdge.from(node3).to(node1)),
                ImmutableSet.of(DirectedEdge.from(node1).to(node2))
        );
        final Map<Node, Node> maxBranching = weightedSpanningTree.val.getParents();
        assertEquals(3, nodeIdVariableAsLong(maxBranching.get(node1)));
        assertEquals(3, nodeIdVariableAsLong(maxBranching.get(node2)));
        assertEquals(0, nodeIdVariableAsLong(maxBranching.get(node3)));
        assertEquals(18.0, weightedSpanningTree.weight, DELTA);
        assertEdgesSumToScore(graph, weightedSpanningTree);

    }

    @Test
    public void testElevenNodeGraph() {


        // make a graph with a bunch of nested cycles so we can exercise the recursive part of the algorithm.
        final WeightedGraph graph = SparseWeightedGraph.from(ImmutableList.of(
                weighted(DirectedEdge.from(node0).to(node8), 0),
                weighted(DirectedEdge.from(node1).to(node2), 10),
                weighted(DirectedEdge.from(node1).to(node4), 5),
                weighted(DirectedEdge.from(node2).to(node3), 9),
                weighted(DirectedEdge.from(node3).to(node1), 8),
                weighted(DirectedEdge.from(node4).to(node5), 9),
                weighted(DirectedEdge.from(node5).to(node6), 10),
                weighted(DirectedEdge.from(node6).to(node4), 8),
                weighted(DirectedEdge.from(node6).to(node7), 5),
                weighted(DirectedEdge.from(node7).to(node8), 10),
                weighted(DirectedEdge.from(node8).to(node2), 5),
                weighted(DirectedEdge.from(node8).to(node9), 8),
                weighted(DirectedEdge.from(node8).to(node10), 1),
                weighted(DirectedEdge.from(node9).to(node7), 9),
                weighted(DirectedEdge.from(node10).to(node3), 3)
        ));
        final Weighted<Arborescence<Node>> weightedSpanningTree = ChuLiuEdmonds.getMaxArborescence(graph, node0);

        final Map<Node, Node> maxBranching = weightedSpanningTree.val.getParents();
        assertEdgesSumToScore(graph, weightedSpanningTree);
        assertEquals(3, nodeIdVariableAsLong(maxBranching.get(node1)));
        assertEquals(8, nodeIdVariableAsLong(maxBranching.get(node2)));
        assertEquals(2, nodeIdVariableAsLong(maxBranching.get(node3)));
        assertEquals(1, nodeIdVariableAsLong(maxBranching.get(node4)));
        assertEquals(5, nodeIdVariableAsLong(maxBranching.get(node6)));
        assertEquals(9, nodeIdVariableAsLong(maxBranching.get(node7)));
        assertEquals(0, nodeIdVariableAsLong(maxBranching.get(node8)));
        assertEquals(8, nodeIdVariableAsLong(maxBranching.get(node9)));
        assertEquals(8, nodeIdVariableAsLong(maxBranching.get(node10)));
    }
}
