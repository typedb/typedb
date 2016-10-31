package io.grakn.test.graql.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.grakn.graql.internal.gremlin.GraqlTraversal;
import io.grakn.graql.internal.gremlin.fragment.Fragment;
import io.grakn.test.AbstractRollbackGraphTest;
import org.junit.Test;

import static io.grakn.graql.internal.gremlin.fragment.Fragments.distinctCasting;
import static io.grakn.graql.internal.gremlin.fragment.Fragments.id;
import static io.grakn.graql.internal.gremlin.fragment.Fragments.inCasting;
import static io.grakn.graql.internal.gremlin.fragment.Fragments.inHasRole;
import static io.grakn.graql.internal.gremlin.fragment.Fragments.inIsa;
import static io.grakn.graql.internal.gremlin.fragment.Fragments.inRolePlayer;
import static io.grakn.graql.internal.gremlin.fragment.Fragments.outCasting;
import static io.grakn.graql.internal.gremlin.fragment.Fragments.outHasRole;
import static io.grakn.graql.internal.gremlin.fragment.Fragments.outIsa;
import static io.grakn.graql.internal.gremlin.fragment.Fragments.outRolePlayer;
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

    @Test
    public void testHasRoleFasterFromRoleType() {
        GraqlTraversal hasRoleFromRelationType = traversal(yId, outHasRole("y", "x"), xId);
        GraqlTraversal hasRoleFromRoleType = traversal(xId, inHasRole("x", "y"), yId);
        assertFaster(hasRoleFromRoleType, hasRoleFromRelationType);
    }

    @Test
    public void testCheckDistinctCastingEarlyFaster() {
        Fragment distinctCasting = distinctCasting("c2", "c1");
        Fragment inRolePlayer = inRolePlayer("x", "c1");
        Fragment inCasting = inCasting("c1", "r");
        Fragment outCasting = outCasting("r", "c2");
        Fragment outRolePlayer = outRolePlayer("c2", "y");

        GraqlTraversal distinctEarly =
                traversal(xId, inRolePlayer, inCasting, outCasting, distinctCasting, outRolePlayer);
        GraqlTraversal distinctLate =
                traversal(xId, inRolePlayer, inCasting, outCasting, outRolePlayer, distinctCasting);

        assertFaster(distinctEarly, distinctLate);
    }

    @Test
    public void testRestartTraversalSlower() {
        // This distinct casting will require restarting the traversal with a new V() step
        Fragment distinctCasting = distinctCasting("c1", "c2");
        Fragment inRolePlayer = inRolePlayer("x", "c1");
        Fragment inCasting = inCasting("c1", "r");
        Fragment outCasting = outCasting("r", "c2");
        Fragment outRolePlayer = outRolePlayer("c2", "y");

        GraqlTraversal distinctEarly =
                traversal(xId, inRolePlayer, inCasting, outCasting, distinctCasting, outRolePlayer);
        GraqlTraversal distinctLate =
                traversal(xId, inRolePlayer, inCasting, outCasting, outRolePlayer, distinctCasting);

        assertFaster(distinctLate, distinctEarly);
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
