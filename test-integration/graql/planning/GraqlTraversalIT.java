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

package grakn.core.graql.planning;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import grakn.core.common.config.Config;
import grakn.core.common.util.Streams;
import grakn.core.core.Schema;
import grakn.core.graql.executor.property.PropertyExecutorFactoryImpl;
import grakn.core.graql.planning.gremlin.fragment.Fragments;
import grakn.core.graql.planning.gremlin.value.ValueOperation;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.planning.gremlin.Fragment;
import grakn.core.kb.graql.planning.gremlin.GraqlTraversal;
import grakn.core.kb.graql.planning.gremlin.TraversalPlanFactory;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.rule.GraknTestStorage;
import grakn.core.rule.SessionUtil;
import grakn.core.rule.TestTransactionProvider;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.property.IdProperty;
import graql.lang.property.SubProperty;
import graql.lang.property.ValueProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static grakn.core.graql.planning.GraqlMatchers.feature;
import static grakn.core.graql.planning.GraqlMatchers.satisfies;
import static grakn.core.graql.planning.gremlin.fragment.Fragments.id;
import static grakn.core.graql.planning.gremlin.fragment.Fragments.inIsa;
import static grakn.core.graql.planning.gremlin.fragment.Fragments.inRelates;
import static grakn.core.graql.planning.gremlin.fragment.Fragments.inSub;
import static grakn.core.graql.planning.gremlin.fragment.Fragments.outIsa;
import static grakn.core.graql.planning.gremlin.fragment.Fragments.outRelates;
import static grakn.core.graql.planning.gremlin.fragment.Fragments.outSub;
import static grakn.core.graql.planning.gremlin.fragment.Fragments.value;
import static graql.lang.Graql.and;
import static graql.lang.Graql.var;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class GraqlTraversalIT {
    @ClassRule
    public static final GraknTestStorage storage = new GraknTestStorage();
    public static Session session;
    private static Transaction tx;

    @BeforeClass
    public static void newSession() {
        Config mockServerConfig = storage.createCompatibleServerConfig();
        session = SessionUtil.serverlessSessionWithNewKeyspace(mockServerConfig);
    }

    @AfterClass
    public static void closeSession() {
        session.close();
    }

    private static final Statement a = Graql.var("a");
    private static final Statement b = Graql.var("b");
    private static final Statement c = Graql.var("c");
    private static final Statement x = Graql.var("x");
    private static final Statement y = Graql.var("y");
    private static final Statement z = Graql.var("z");
    private static final Statement xx = Graql.var("xx");
    private static final Fragment xId = id(null, x.var(), ConceptId.of("Titanic"));
    private static final Fragment yId = id(null, y.var(), ConceptId.of("movie"));
    private static final Fragment xIsaY = outIsa(null, x.var(), y.var());
    private static final Fragment yTypeOfX = inIsa(null, y.var(), x.var(), true);

    private static final GraqlTraversal fastIsaTraversal = traversal(yId, yTypeOfX);
    private final String ROLE_PLAYER_EDGE = Schema.EdgeLabel.ROLE_PLAYER.getLabel();

    @Before
    public void setUp() {
        tx = session.writeTransaction();
        Role wife = tx.putRole("wife");
        EntityType personType = tx.putEntityType("person").plays(wife);
        RelationType marriageType = tx.putRelationType("marriage").relates(wife);
        tx.commit();
        tx = session.writeTransaction();
    }

    @After
    public void closeTransaction() {
        tx.close();
    }

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
        GraqlTraversal connectedDoubleIsa = traversal(xIsaY, outIsa(null, y.var(), z.var()));
        GraqlTraversal disconnectedDoubleIsa = traversal(xIsaY, inIsa(null, z.var(), y.var(), true));
        assertFaster(connectedDoubleIsa, disconnectedDoubleIsa);
    }

    @Test
    public void testGloballyOptimalIsFasterThanLocallyOptimal() {
        GraqlTraversal locallyOptimalSpecificInstance = traversal(yId, yTypeOfX, xId);
        GraqlTraversal globallyOptimalSpecificInstance = traversal(xId, xIsaY, yId);
        assertFaster(globallyOptimalSpecificInstance, locallyOptimalSpecificInstance);
    }

    @Test
    public void testRelatesFasterFromRoleType() {
        GraqlTraversal relatesFromRelationType = traversal(yId, outRelates(null, y.var(), x.var()), xId);
        GraqlTraversal relatesFromRoleType = traversal(xId, inRelates(null, x.var(), y.var()), yId);
        assertFaster(relatesFromRoleType, relatesFromRelationType);
    }

    @Test
    public void testResourceWithTypeFasterFromType() {
        GraqlTraversal fromInstance =
                traversal(outIsa(null, x.var(), xx.var()), id(null, xx.var(), ConceptId.of("_")), inRolePlayer(x.var(), z.var()), outRolePlayer(z.var(), y.var()));
        GraqlTraversal fromType =
                traversal(id(null, xx.var(), ConceptId.of("_")), inIsa(null, xx.var(), x.var(), true), inRolePlayer(x.var(), z.var()), outRolePlayer(z.var(), y.var()));
        assertFaster(fromType, fromInstance);
    }

    @Ignore //TODO: No longer applicable. Think of a new test to replace this.
    @Test
    public void valueFilteringIsBetterThanANonFilteringOperation() {
        ValueOperation<?,?> gt_1 = ValueOperation.of(ValueProperty.Operation.Comparison.of(Graql.Token.Comparator.GT, 1));
        GraqlTraversal valueFilterFirst = traversal(value(null, x.var(), gt_1), inRolePlayer(x.var(), b.var()), outRolePlayer(b.var(), y.var()), outIsa(null, y.var(), z.var()));
        GraqlTraversal rolePlayerFirst = traversal(outIsa(null, y.var(), z.var()), inRolePlayer(y.var(), b.var()), outRolePlayer(b.var(), x.var()), value(null, x.var(), gt_1));

        assertFaster(valueFilterFirst, rolePlayerFirst);
    }

    @Test
    public void testAllTraversalsSimpleQuery() {
        IdProperty titanicId = new IdProperty("Titanic");
        IdProperty movieId = new IdProperty("movie");
        SubProperty subProperty = new SubProperty(new Statement(y.var(), ImmutableList.of(movieId)));

        Statement pattern = new Statement(x.var(), ImmutableList.of(titanicId, subProperty));
        Set<GraqlTraversal> traversals = allGraqlTraversals(pattern).collect(toSet());

        assertEquals(12, traversals.size());

        Fragment xId = id(titanicId, x.var(), ConceptId.of("Titanic"));
        Fragment yId = id(movieId, y.var(), ConceptId.of("movie"));
        Fragment xSubY = outSub(subProperty, x.var(), y.var(), Fragments.TRAVERSE_ALL_SUB_EDGES);
        Fragment ySubX = inSub(subProperty, y.var(), x.var(), Fragments.TRAVERSE_ALL_SUB_EDGES);

        Set<GraqlTraversal> expected = ImmutableSet.of(
                traversal(xId, xSubY, yId),
                traversal(xId, ySubX, yId),
                traversal(xId, yId, xSubY),
                traversal(xId, yId, ySubX),
                traversal(xSubY, xId, yId),
                traversal(xSubY, yId, xId),
                traversal(ySubX, xId, yId),
                traversal(ySubX, yId, xId),
                traversal(yId, xId, xSubY),
                traversal(yId, xId, ySubX),
                traversal(yId, xSubY, xId),
                traversal(yId, ySubX, xId)
        );

        assertEquals(expected, traversals);
    }

    @Test
    public void testOptimalShortQuery() {
        assertNearlyOptimal(x.isa(y.id("movie")));
    }

    @Test
    public void testOptimalBothId() {
        assertNearlyOptimal(x.id("Titanic").isa(y.id("movie")));
    }

    @Test
    public void testOptimalByValue() {
        assertNearlyOptimal(x.val("hello").isa(y.id("movie")));
    }

    @Ignore // TODO: This is now super-slow
    @Test
    public void testOptimalAttachedResource() {
        assertNearlyOptimal(var()
                                    .rel(x.isa(y.id("movie")))
                                    .rel(z.val("Titanic").isa(var("a").id("title"))));
    }

    @Ignore // TODO: This is now super-slow
    @Test
    public void makeSureTypeIsCheckedBeforeFollowingARolePlayer() {
        assertNearlyOptimal(and(
                x.id("xid"),
                var().rel(x).rel(y),
                y.isa(b.type("person")),
                var().rel(y).rel(z)
        ));
    }

    @Ignore("Need to build proper mocks")
    @Test
    public void whenPlanningSimpleUnaryRelation_ApplyRolePlayerOptimisation() {
        Statement rel = var("x").rel("y");

        GraqlTraversal graqlTraversal = semiOptimal(rel);

        // I know this is horrible, unfortunately I can't think of a better way...
        // The issue is that some things we want to inspect are not public, mainly:
        // 1. The variable name assigned to the casting
        // 2. The role-player fragment classes
        // Both of these things should not be made public if possible, so I see this regex as the lesser evil
        assertThat(graqlTraversal, anyOf(
                matches("\\{§x-\\[" + ROLE_PLAYER_EDGE + ":#.*]->§y}"),
                matches("\\{§y<-\\[" + ROLE_PLAYER_EDGE + ":#.*]-§x}")
        ));
    }

    @Ignore("Need to build proper mocks")
    @Test
    public void whenPlanningSimpleBinaryRelationQuery_ApplyRolePlayerOptimisation() {
        Statement rel = var("x").rel("y").rel("z");

        GraqlTraversal graqlTraversal = semiOptimal(rel);

        assertThat(graqlTraversal, anyOf(
                matches("\\{§x-\\[" + ROLE_PLAYER_EDGE + ":#.*]->§.* §x-\\[" + ROLE_PLAYER_EDGE + ":#.*]->§.* #.*\\[neq:#.*]}"),
                matches("\\{§.*<-\\[" + ROLE_PLAYER_EDGE + ":#.*]-§x-\\[" + ROLE_PLAYER_EDGE + ":#.*]->§.* #.*\\[neq:#.*]}")
        ));
    }

    @Ignore("Need to build proper mocks")
    @Test
    public void whenPlanningBinaryRelationQueryWithType_ApplyRolePlayerOptimisation() {
        Statement rel = var("x").rel("y").rel("z").isa("marriage");

        GraqlTraversal graqlTraversal = semiOptimal(rel);

        assertThat(graqlTraversal, anyOf(
                matches(".*§x-\\[" + ROLE_PLAYER_EDGE + ":#.* rels:marriage]->§.* §x-\\[" + ROLE_PLAYER_EDGE + ":#.* rels:marriage]->§.* #.*\\[neq:#.*].*"),
                matches(".*§.*<-\\[" + ROLE_PLAYER_EDGE + ":#.* rels:marriage]-§x-\\[" + ROLE_PLAYER_EDGE + ":#.* rels:marriage]->§.* #.*\\[neq:#.*].*")
        ));
    }

    @Ignore("Need to build proper mocks")
    @Test
    public void testRolePlayerOptimisationWithRoles() {
        Statement rel = var("x").rel("y").rel("wife", "z");

        GraqlTraversal graqlTraversal = semiOptimal(rel);

        assertThat(graqlTraversal, anyOf(
                matches(".*§x-\\[" + ROLE_PLAYER_EDGE + ":#.* roles:wife]->§.* §x-\\[" + ROLE_PLAYER_EDGE + ":#.*]->§.* #.*\\[neq:#.*].*"),
                matches(".*§.*<-\\[" + ROLE_PLAYER_EDGE + ":#.* roles:wife]-§x-\\[" + ROLE_PLAYER_EDGE + ":#.*]->§.* #.*\\[neq:#.*].*")
        ));
    }

    private static GraqlTraversal semiOptimal(Pattern pattern) {
        TraversalPlanFactory traversalPlanFactory = ((TestTransactionProvider.TestTransaction)tx).traversalPlanFactory();
        return traversalPlanFactory.createTraversal(pattern);
    }

    private static GraqlTraversal traversal(Fragment... fragments) {
        return traversal(ImmutableList.copyOf(fragments));
    }

    @SafeVarargs
    private static GraqlTraversal traversal(ImmutableList<Fragment>... fragments) {
        Set<List<? extends Fragment>> fragmentsSet = ImmutableSet.copyOf(fragments);
        return new GraqlTraversalImpl(null, null, fragmentsSet);
    }

    private static Stream<GraqlTraversal> allGraqlTraversals(Pattern pattern) {
        Collection<Conjunction<Statement>> patterns = pattern.getDisjunctiveNormalForm().getPatterns();

        ConceptManager conceptManager = ((TestTransactionProvider.TestTransaction)tx).conceptManager();
        List<Set<List<Fragment>>> collect = patterns.stream()
                .map(conjunction -> new ConjunctionQuery(conjunction, conceptManager, new PropertyExecutorFactoryImpl()))
                .map(ConjunctionQuery::allFragmentOrders)
                .collect(toList());

        Set<List<List<? extends Fragment>>> lists = Sets.cartesianProduct(collect);

        return lists.stream()
                .map(Sets::newHashSet)
                .map(GraqlTraversalIT::createTraversal)
                .flatMap(Streams::optionalToStream);
    }

    // Returns a traversal only if the fragment ordering is valid
    private static Optional<GraqlTraversal> createTraversal(Set<List<? extends Fragment>> fragments) {

        // Make sure all dependencies are met
        for (List<? extends Fragment> fragmentList : fragments) {
            Set<Variable> visited = new HashSet<>();

            for (Fragment fragment : fragmentList) {
                if (!visited.containsAll(fragment.dependencies())) {
                    return Optional.empty();
                }

                visited.addAll(fragment.vars());
            }
        }

        return Optional.of(new GraqlTraversalImpl(null, null, fragments));
    }

    private static Fragment outRolePlayer(Variable relation, Variable rolePlayer) {
        return Fragments.outRolePlayer(null, relation, a.var(), rolePlayer, null, null, null);
    }

    private static Fragment inRolePlayer(Variable rolePlayer, Variable relation) {
        return Fragments.inRolePlayer(null, rolePlayer, c.var(), relation, null, null, null);
    }

    private static void assertNearlyOptimal(Pattern pattern) {
        GraqlTraversal traversal = semiOptimal(pattern);

        //noinspection OptionalGetWithoutIsPresent
        GraqlTraversal globalOptimum = allGraqlTraversals(pattern).min(comparing(GraqlTraversal::getComplexity)).get();

        double globalComplexity = globalOptimum.getComplexity();
        double complexity = traversal.getComplexity();

        assertTrue(
                "Expected\n " +
                        complexity + ":\t" + traversal + "\nto be similar speed to\n " +
                        globalComplexity + ":\t" + globalOptimum,
                complexity - globalComplexity <= 0.01
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

    private <T> Matcher<T> matches(String regex) {
        return feature(satisfies(string -> string.matches(regex)), "matching " + regex, Object::toString);
    }
}