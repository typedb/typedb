/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.test.graql.query;

import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;
import ai.grakn.graql.internal.gremlin.GraqlTraversal;
import ai.grakn.graql.internal.gremlin.GremlinQuery;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.test.AbstractRollbackGraphTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.util.Optional;
import java.util.Set;

import static ai.grakn.graql.Graql.*;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.*;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.id;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GraqlTraversalTest extends AbstractRollbackGraphTest {

    private static final Fragment xId = id("x", "Titanic");
    private static final Fragment xValue = value("x", eq("hello").admin());
    private static final Fragment yId = id("y", "movie");
    private static final Fragment xIsaY = outIsa("x", "y");
    private static final Fragment yTypeOfX = inIsa("y", "x");
    private static final Fragment xShortcutY = shortcut(Optional.empty(), Optional.empty(), Optional.empty(), "x", "y");
    private static final Fragment yShortcutX = shortcut(Optional.empty(), Optional.empty(), Optional.empty(), "y", "x");

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
    public void testResourceWithTypeFasterFromType() {
        GraqlTraversal fromInstance =
                traversal(outIsa("x", "X"), id("X", "_"), makeShortcut("x", "y"), outIsa("y", "Y"), id("Y", "_"));
        GraqlTraversal fromType =
                traversal(id("X", "_"), inIsa("X", "x"), makeShortcut("x", "y"), outIsa("y", "Y"), id("Y", "_"));
        assertFaster(fromType, fromInstance);
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
    public void testAllTraversalsSimpleQuery() {
        Var pattern = Patterns.var("x").id("Titanic").isa(Patterns.var("y").id("movie"));
        GremlinQuery query = new GremlinQuery(graph, pattern.admin(), ImmutableSet.of("x"), Optional.empty());

        Set<GraqlTraversal> traversals = query.allGraqlTraversals().collect(toSet());

        assertEquals(12, traversals.size());

        Set<GraqlTraversal> expected = ImmutableSet.of(
                traversal(xId, xIsaY, yId),
                traversal(xId, yTypeOfX, yId),
                traversal(xId, yId, xIsaY),
                traversal(xId, yId, yTypeOfX),
                traversal(xIsaY, xId, yId),
                traversal(xIsaY, yId, xId),
                traversal(yTypeOfX, xId, yId),
                traversal(yTypeOfX, yId, xId),
                traversal(yId, xId, xIsaY),
                traversal(yId, xId, yTypeOfX),
                traversal(yId, xIsaY, xId),
                traversal(yId, yTypeOfX, xId)
        );

        assertEquals(expected, traversals);
    }

    @Test
    public void testAllTraversalsDisjunction() {
        Pattern pattern = or(Patterns.var("x").id("Titanic").value("hello"), Patterns.var().rel("x").rel("y"));
        GremlinQuery query = new GremlinQuery(graph, pattern.admin(), ImmutableSet.of("x"), Optional.empty());

        Set<GraqlTraversal> traversals = query.allGraqlTraversals().collect(toSet());

        // Expect all combinations of both disjunctions
        Set<GraqlTraversal> expected = ImmutableSet.of(
                traversal(ImmutableList.of(xId, xValue), ImmutableList.of(xShortcutY)),
                traversal(ImmutableList.of(xId, xValue), ImmutableList.of(yShortcutX)),
                traversal(ImmutableList.of(xValue, xId), ImmutableList.of(xShortcutY)),
                traversal(ImmutableList.of(xValue, xId), ImmutableList.of(yShortcutX))
        );

        assertEquals(expected, traversals);
    }

    @Test
    public void testOptimalShortQuery() {
        assertNearlyOptimal(var("x").isa(var("y").id("movie")));
    }

    @Test
    public void testOptimalBothId() {
        assertNearlyOptimal(var("x").id("Titanic").isa(var("y").id("movie")));
    }

    @Test
    public void testOptimalByValue() {
        assertNearlyOptimal(var("x").value("hello").isa(var("y").id("movie")));
    }

    @Test
    public void testOptimalAttachedResource() {
        assertNearlyOptimal(var()
                .rel(var("x").isa(var("y").id("movie")))
                .rel(var("z").value("Titanic").isa(var("a").id("title"))));
    }

    private static GraqlTraversal optimalTraversal(Pattern pattern) {
        return new GremlinQuery(graph, pattern.admin(), ImmutableSet.of(), Optional.empty()).optimalTraversal();
    }

    private static GraqlTraversal traversal(Fragment... fragments) {
        return traversal(ImmutableList.copyOf(fragments));
    }

    @SafeVarargs
    private static GraqlTraversal traversal(ImmutableList<Fragment>... fragments) {
        ImmutableSet<ImmutableList<Fragment>> fragmentsSet = ImmutableSet.copyOf(fragments);
        return GraqlTraversal.create(graph, fragmentsSet);
    }

    private static Fragment makeShortcut(String x, String y) {
        return shortcut(Optional.empty(), Optional.empty(), Optional.empty(), x, y);
    }

    private static void assertNearlyOptimal(Pattern pattern) {
        GremlinQuery query = new GremlinQuery(graph, pattern.admin(), ImmutableSet.of("x"), Optional.empty());

        GraqlTraversal traversal = optimalTraversal(pattern);

        //noinspection OptionalGetWithoutIsPresent
        GraqlTraversal globalOptimum = query.allGraqlTraversals().min(comparing(GraqlTraversal::getComplexity)).get();

        double globalComplexity = globalOptimum.getComplexity();
        double complexity = traversal.getComplexity();

        assertTrue(
                "Expected\n " +
                        complexity + ":\t" + traversal + "\nto be similar speed to\n " +
                        globalComplexity + ":\t" + globalOptimum,
                complexity < globalComplexity * 2
        );
    }

    private static void assertFaster(GraqlTraversal fast, GraqlTraversal slow) {
        double fastComplexity = fast.getComplexity();
        double slowComplexity = slow.getComplexity();
        boolean condition = fastComplexity < slowComplexity;

        assertTrue(
                "Expected\n" + fastComplexity + ":\t" + fast + "\nto be faster than\n" + slowComplexity + ":\t" + slow,
                condition
        );
    }
}
