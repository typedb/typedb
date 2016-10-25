package io.mindmaps.test.graql.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.mindmaps.graql.internal.gremlin.GraqlTraversal;
import io.mindmaps.graql.internal.gremlin.fragment.Fragment;
import io.mindmaps.test.AbstractRollbackGraphTest;
import org.junit.Test;

import static io.mindmaps.graql.internal.gremlin.fragment.Fragments.id;
import static io.mindmaps.graql.internal.gremlin.fragment.Fragments.inIsa;
import static io.mindmaps.graql.internal.gremlin.fragment.Fragments.outIsa;
import static org.junit.Assert.assertTrue;

public class GraqlTraversalTest extends AbstractRollbackGraphTest {

    private static final Fragment xId = id("x", "Titanic");
    private static final Fragment yId = id("y", "movie");
    private static final Fragment xIsaY = outIsa("x", "y");
    private static final Fragment yTypeOfX = inIsa("y", "x");

    private static final GraqlTraversal fastIsaTraversal = traversal(yId, yTypeOfX);

    @Test
    public void testComplexityIndexVsIsa() {
        GraqlTraversal indexTraversal = traversal(xId);
        assertFaster(indexTraversal, fastIsaTraversal);
    }

    @Test
    public void testComplexityFastIsaVsSlowIsa() {
        GraqlTraversal slowIsaTraversal = traversal(xIsaY, yId);
        assertFaster(fastIsaTraversal, slowIsaTraversal);
    }

    @Test
    public void testComplexityConnectedVsDisconnected() {
        GraqlTraversal connectedDoubleIsa = traversal(xIsaY, outIsa("y", "z"));
        GraqlTraversal disconnectedDoubleIsa = traversal(xIsaY, inIsa("z", "y"));
        assertFaster(connectedDoubleIsa, disconnectedDoubleIsa);
    }

    @Test
    public void testGloballyOptimalIsFasterThanLocallyOptimal() {
        GraqlTraversal locallyOptimalSpecificInstance = traversal(yId, yTypeOfX, xId);
        GraqlTraversal globallyOptimalSpecificInstance = traversal(xId, xIsaY, yId);
        assertFaster(globallyOptimalSpecificInstance, locallyOptimalSpecificInstance);
    }

    private static GraqlTraversal traversal(Fragment... fragments) {
        ImmutableSet<ImmutableList<Fragment>> fragmentsSet = ImmutableSet.of(ImmutableList.copyOf(fragments));
        return GraqlTraversal.create(graph, fragmentsSet);
    }

    private static void assertFaster(GraqlTraversal fast, GraqlTraversal slow) {
        long fastComplexity = fast.getComplexity();
        long slowComplexity = slow.getComplexity();
        boolean condition = fastComplexity < slowComplexity;

        assertTrue(
                "Expected\n" + fastComplexity + ":\t" + fast + "\nto be faster than\n" + slowComplexity + ":\t" + slow,
                condition
        );
    }
}
