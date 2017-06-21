package ai.grakn.graql.internal.gremlin.spanningtree;

import ai.grakn.graql.internal.gremlin.spanningtree.datastructure.Partition;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.DirectedEdge;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.SparseWeightedGraph;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.WeightedGraph;
import ai.grakn.graql.internal.gremlin.spanningtree.util.Weighted;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class KBestArborescencesTest {
    private final static ImmutableSet<DirectedEdge<Integer>> empty = ImmutableSet.of();
    // tied for first, can appear in either order
    private final static Weighted<Arborescence<Integer>> bestA = Weighted.weighted(Arborescence.of(ImmutableMap.of(
            1, 0,
            2, 1,
            3, 2
    )), 21);
    private final static Weighted<Arborescence<Integer>> bestB = Weighted.weighted(Arborescence.of(ImmutableMap.of(
            1, 3,
            2, 1,
            3, 0
    )), 21);
    final static Set<Weighted<Arborescence<Integer>>> expectedFirstAndSecond = ImmutableSet.of(bestA, bestB);

    @Test
    public void testGetKBestArborescences() {
        final List<Weighted<Arborescence<Integer>>> weightedSpanningTrees =
                KBestArborescences.getKBestArborescences(ChuLiuEdmondsTest.graph, 0, 4);

        assertEquals(4, ImmutableSet.copyOf(weightedSpanningTrees).size());

        Weighted<Arborescence<Integer>> weightedSpanningTree = weightedSpanningTrees.get(0);
        assertTrue(expectedFirstAndSecond.contains(weightedSpanningTree));
        ChuLiuEdmondsTest.assertEdgesSumToScore(ChuLiuEdmondsTest.graph, weightedSpanningTree);

        weightedSpanningTree = weightedSpanningTrees.get(1);
        assertTrue(expectedFirstAndSecond.contains(weightedSpanningTree));
        ChuLiuEdmondsTest.assertEdgesSumToScore(ChuLiuEdmondsTest.graph, weightedSpanningTree);

        weightedSpanningTree = weightedSpanningTrees.get(2);
        final Arborescence<Integer> expectedThird = Arborescence.of(ImmutableMap.of(
                1, 0,
                2, 1,
                3, 1));
        Assert.assertEquals(Weighted.weighted(expectedThird, 20), weightedSpanningTree);
        ChuLiuEdmondsTest.assertEdgesSumToScore(ChuLiuEdmondsTest.graph, weightedSpanningTree);

        weightedSpanningTree = weightedSpanningTrees.get(3);
        final Arborescence<Integer> expectedFourth = Arborescence.of(ImmutableMap.of(
                1, 2,
                2, 3,
                3, 0));
        Assert.assertEquals(Weighted.weighted(expectedFourth, 19.0), weightedSpanningTree);
        ChuLiuEdmondsTest.assertEdgesSumToScore(ChuLiuEdmondsTest.graph, weightedSpanningTree);
    }

    @Test
    public void testGetLotsOfKBest() {
        final int k = 100;
        final List<Weighted<Arborescence<Integer>>> kBestSpanningTrees =
                KBestArborescences.getKBestArborescences(ChuLiuEdmondsTest.graph, 0, k);
        final int size = kBestSpanningTrees.size();
        // make sure there are no more than k of them
        assertTrue(size <= k);
        // make sure they are in descending order
        for (int i = 0; i + 1 < size; i++) {
            assertTrue(kBestSpanningTrees.get(i).weight >= kBestSpanningTrees.get(i + 1).weight);
        }
        for (Weighted<Arborescence<Integer>> spanningTree : kBestSpanningTrees) {
            ChuLiuEdmondsTest.assertEdgesSumToScore(ChuLiuEdmondsTest.graph, spanningTree);
        }
        // make sure they're all unique
        assertEquals(size, ImmutableSet.copyOf(kBestSpanningTrees).size());
    }

    @Test
    public void testSeekDoesntReturnAncestor() {
        final Weighted<Arborescence<Integer>> bestArborescence = bestA;
        final ExclusiveEdge<Integer> maxInEdge = ExclusiveEdge.of(DirectedEdge.from(1).to(2), 11.0);
        final EdgeQueueMap.EdgeQueue<Integer> edgeQueue =
                EdgeQueueMap.EdgeQueue.create(maxInEdge.edge.destination, Partition.singletons(ChuLiuEdmondsTest.graph.getNodes()));
        edgeQueue.addEdge(ExclusiveEdge.of(DirectedEdge.from(0).to(2), 1.0));
        edgeQueue.addEdge(ExclusiveEdge.of(DirectedEdge.from(3).to(2), 8.0));
        final Optional<ExclusiveEdge<Integer>> nextBestEdge =
                KBestArborescences.seek(maxInEdge, bestArborescence.val, edgeQueue);
        assertTrue(nextBestEdge.isPresent());
        // 3 -> 2 is an ancestor in bestArborescence, so seek should not return it
        Assert.assertNotEquals(DirectedEdge.from(3).to(2), nextBestEdge.get().edge);
        Assert.assertEquals(DirectedEdge.from(0).to(2), nextBestEdge.get().edge);
    }

    @Test
    public void testSeek() {
        final Arborescence<Integer> best = Arborescence.of(ImmutableMap.of(
                2, 0,
                1, 2,
                3, 2
        ));
        final ExclusiveEdge<Integer> maxInEdge = ExclusiveEdge.of(DirectedEdge.from(2).to(1), 10.0);
        final EdgeQueueMap.EdgeQueue<Integer> edgeQueue =
                EdgeQueueMap.EdgeQueue.create(maxInEdge.edge.destination, Partition.singletons(ChuLiuEdmondsTest.graph.getNodes()));
        edgeQueue.addEdge(ExclusiveEdge.of(DirectedEdge.from(0).to(1), 5.0));
        edgeQueue.addEdge(ExclusiveEdge.of(DirectedEdge.from(3).to(1), 9.0));
        final Optional<ExclusiveEdge<Integer>> nextBestEdge = KBestArborescences.seek(maxInEdge, best, edgeQueue);
        assertTrue(nextBestEdge.isPresent());
        Assert.assertEquals(DirectedEdge.from(3).to(1), nextBestEdge.get().edge);
        Assert.assertEquals(9.0, nextBestEdge.get().weight, ChuLiuEdmondsTest.DELTA);
    }

    @Test
    public void testNext() {
        final Optional<Weighted<KBestArborescences.SubsetOfSolutions<Integer>>> oItem =
                KBestArborescences.scoreSubsetOfSolutions(ChuLiuEdmondsTest.graph, empty, empty, bestA);
        assertTrue(oItem.isPresent());
        final KBestArborescences.SubsetOfSolutions<Integer> item = oItem.get().val;
        Assert.assertEquals(DirectedEdge.from(0).to(1), item.edgeToBan);
        Assert.assertEquals(0.0, item.bestArborescence.weight - oItem.get().weight, ChuLiuEdmondsTest.DELTA);
    }

    @Test
    public void testNextWithRequiredEdges() {
        final Optional<Weighted<KBestArborescences.SubsetOfSolutions<Integer>>> oItem =
                KBestArborescences.scoreSubsetOfSolutions(ChuLiuEdmondsTest.graph, ImmutableSet.of(DirectedEdge.from(0).to(1)), empty, bestA);
        assertTrue(oItem.isPresent());
        final KBestArborescences.SubsetOfSolutions<Integer> item = oItem.get().val;
        Assert.assertEquals(DirectedEdge.from(2).to(3), item.edgeToBan);
        Assert.assertEquals(1.0, item.bestArborescence.weight - oItem.get().weight, ChuLiuEdmondsTest.DELTA);
    }

    @Test
    public void testNextReturnsAbsentWhenTreesAreExhausted() {
        final WeightedGraph<Integer> oneTreeGraph = SparseWeightedGraph.from(
                ImmutableSet.of(Weighted.weighted(DirectedEdge.from(0).to(1), 1.0))
        );
        final Weighted<Arborescence<Integer>> best = ChuLiuEdmonds.getMaxArborescence(oneTreeGraph, 0);
        final Optional<Weighted<KBestArborescences.SubsetOfSolutions<Integer>>> pair =
                KBestArborescences.scoreSubsetOfSolutions(oneTreeGraph, empty, empty, best);
        assertFalse(pair.isPresent());
    }
}
