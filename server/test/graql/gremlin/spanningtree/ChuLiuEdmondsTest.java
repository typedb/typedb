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

package grakn.core.graql.gremlin.spanningtree;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import grakn.core.graql.gremlin.spanningtree.graph.DenseWeightedGraph;
import grakn.core.graql.gremlin.spanningtree.graph.DirectedEdge;
import grakn.core.graql.gremlin.spanningtree.graph.Node;
import grakn.core.graql.gremlin.spanningtree.graph.NodeId;
import grakn.core.graql.gremlin.spanningtree.graph.SparseWeightedGraph;
import grakn.core.graql.gremlin.spanningtree.graph.WeightedGraph;
import grakn.core.graql.gremlin.spanningtree.util.Weighted;
import graql.lang.statement.Variable;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static grakn.core.graql.gremlin.spanningtree.util.Weighted.weighted;
import static org.junit.Assert.assertEquals;


public class ChuLiuEdmondsTest {
    final static double DELTA = 0.001;
    final static double NINF = Double.NEGATIVE_INFINITY;

    static Map<NodeId, Node> nodes = new HashMap<>();
    static Node node0 = Node.addIfAbsent(NodeId.NodeType.VAR, new Variable("0"), nodes);
    static Node node1 = Node.addIfAbsent(NodeId.NodeType.VAR, new Variable("1"), nodes);
    static Node node2 = Node.addIfAbsent(NodeId.NodeType.VAR, new Variable("2"), nodes);
    static Node node3 = Node.addIfAbsent(NodeId.NodeType.VAR, new Variable("3"), nodes);
    static Node node4 = Node.addIfAbsent(NodeId.NodeType.VAR, new Variable("4"), nodes);
    static Node node5 = Node.addIfAbsent(NodeId.NodeType.VAR, new Variable("5"), nodes);
    static Node node6 = Node.addIfAbsent(NodeId.NodeType.VAR, new Variable("6"), nodes);
    static Node node7 = Node.addIfAbsent(NodeId.NodeType.VAR, new Variable("7"), nodes);
    static Node node8 = Node.addIfAbsent(NodeId.NodeType.VAR, new Variable("8"), nodes);
    static Node node9 = Node.addIfAbsent(NodeId.NodeType.VAR, new Variable("9"), nodes);
    static Node node10 = Node.addIfAbsent(NodeId.NodeType.VAR, new Variable("10"), nodes);

    final static WeightedGraph graph = SparseWeightedGraph.from(ImmutableList.of(
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
        Map<NodeId, Node> nodes = new HashMap<>();
        Node node0 = Node.addIfAbsent(NodeId.NodeType.VAR, new Variable("0"), nodes);
        Node node1 = Node.addIfAbsent(NodeId.NodeType.VAR, new Variable("1"), nodes);
        Node node2 = Node.addIfAbsent(NodeId.NodeType.VAR, new Variable("2"), nodes);
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
