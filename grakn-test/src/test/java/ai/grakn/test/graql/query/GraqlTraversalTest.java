package ai.grakn.test.graql.query;

import ai.grakn.graql.internal.gremlin.fragment.Fragment;
import ai.grakn.graql.internal.gremlin.fragment.Fragments;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import ai.grakn.graql.internal.gremlin.GraqlTraversal;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;
import ai.grakn.test.AbstractRollbackGraphTest;
import org.junit.Test;

import static ai.grakn.graql.internal.gremlin.fragment.Fragments.distinctCasting;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.id;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.inCasting;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.inHasRole;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.inIsa;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.inRolePlayer;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.outCasting;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.outHasRole;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.outIsa;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.outRolePlayer;
import static org.junit.Assert.assertTrue;

public class GraqlTraversalTest extends AbstractRollbackGraphTest {

    private static final Fragment xId = Fragments.id("x", "Titanic");
    private static final Fragment yId = Fragments.id("y", "movie");
    private static final Fragment xIsaY = Fragments.outIsa("x", "y");
    private static final Fragment yTypeOfX = Fragments.inIsa("y", "x");

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
        GraqlTraversal connectedDoubleIsa = traversal(xIsaY, Fragments.outIsa("y", "z"));
        GraqlTraversal disconnectedDoubleIsa = traversal(xIsaY, Fragments.inIsa("z", "y"));
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
        GraqlTraversal hasRoleFromRelationType = traversal(yId, Fragments.outHasRole("y", "x"), xId);
        GraqlTraversal hasRoleFromRoleType = traversal(xId, Fragments.inHasRole("x", "y"), yId);
        assertFaster(hasRoleFromRoleType, hasRoleFromRelationType);
    }

    @Test
    public void testCheckDistinctCastingEarlyFaster() {
        Fragment distinctCasting = Fragments.distinctCasting("c2", "c1");
        Fragment inRolePlayer = Fragments.inRolePlayer("x", "c1");
        Fragment inCasting = Fragments.inCasting("c1", "r");
        Fragment outCasting = Fragments.outCasting("r", "c2");
        Fragment outRolePlayer = Fragments.outRolePlayer("c2", "y");

        GraqlTraversal distinctEarly =
                traversal(xId, inRolePlayer, inCasting, outCasting, distinctCasting, outRolePlayer);
        GraqlTraversal distinctLate =
                traversal(xId, inRolePlayer, inCasting, outCasting, outRolePlayer, distinctCasting);

        assertFaster(distinctEarly, distinctLate);
    }

    @Test
    public void testRestartTraversalSlower() {
        // This distinct casting will require restarting the traversal with a new V() step
        Fragment distinctCasting = Fragments.distinctCasting("c1", "c2");
        Fragment inRolePlayer = Fragments.inRolePlayer("x", "c1");
        Fragment inCasting = Fragments.inCasting("c1", "r");
        Fragment outCasting = Fragments.outCasting("r", "c2");
        Fragment outRolePlayer = Fragments.outRolePlayer("c2", "y");

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
