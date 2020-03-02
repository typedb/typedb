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
 *
 */

package grakn.core.graql.reasoner.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.collect.UnmodifiableIterator;
import grakn.core.common.config.Config;
import grakn.core.graql.planning.NodesUtil;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.atom.predicate.IdPredicate;
import grakn.core.graql.reasoner.plan.ResolutionPlan;
import grakn.core.graql.reasoner.plan.ResolutionQueryPlan;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.graql.planning.gremlin.TraversalPlanFactory;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import grakn.core.kb.keyspace.KeyspaceStatistics;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.keyspace.KeyspaceStatisticsImpl;
import grakn.core.rule.GraknTestStorage;
import grakn.core.rule.SessionUtil;
import grakn.core.rule.TestTransactionProvider;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import static grakn.core.util.GraqlTestUtil.loadFromFileAndCommit;
import static graql.lang.Graql.var;
import static java.lang.annotation.ElementType.METHOD;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("Duplicates")
public class ResolutionPlanIT {

    private static final int repeat = 20;

    @Rule
    public RepeatRule repeatRule = new RepeatRule();

    @ClassRule
    public static final GraknTestStorage storage = new GraknTestStorage();

    private static Session planSession;
    private static KeyspaceStatistics planKeyspaceStatistics;
    private Transaction tx;

    // factories tied to a tx that we can utilise directly in tests
    private ReasonerQueryFactory reasonerQueryFactory;
    private TraversalPlanFactory traversalPlanFactory;

    @BeforeClass
    public static void loadContext(){
        Config mockServerConfig = storage.createCompatibleServerConfig();
        planKeyspaceStatistics = new KeyspaceStatisticsImpl();
        planSession = SessionUtil.serverlessSessionWithNewKeyspace(mockServerConfig, planKeyspaceStatistics);
        String resourcePath = "test-integration/graql/reasoner/resources/";
        loadFromFileAndCommit(resourcePath, "resolutionPlanTest.gql", planSession);
    }

    @AfterClass
    public static void closeSession(){
        planSession.close();
    }

    @Before
    public void setUp(){
        tx = planSession.writeTransaction();
        TestTransactionProvider.TestTransaction testTx = ((TestTransactionProvider.TestTransaction)tx);
        reasonerQueryFactory = testTx.reasonerQueryFactory();
        traversalPlanFactory = testTx.traversalPlanFactory();
    }

    @After
    public void tearDown(){
        tx.close();
    }

    @Test
    @Repeat( times = repeat )
    public void whenDisconnectedIndexedQueriesPresentWithIndexedResource_completePlanIsProduced() {
        String queryString = "{" +
                "$x isa someEntity;" +
                "$y isa resource;$y 'value';" +
                "$z isa someRelation;" +
                "};";
        ReasonerQueryImpl query = reasonerQueryFactory.create(conjunction(queryString));
        checkQueryPlanComplete(query, new ResolutionQueryPlan(reasonerQueryFactory, query));
    }

    @Test
    @Repeat( times = repeat )
    public void whenDisconnectedIndexedQueriesPresentWithIndexedEntity_completePlanIsProduced() {
        String queryString = "{" +
                "$x isa someEntity;$x id V123;" +
                "$y isa resource;" +
                "$z isa someRelation;" +
                "};";
        ReasonerQueryImpl query = reasonerQueryFactory.create(conjunction(queryString));
        checkQueryPlanComplete(query, new ResolutionQueryPlan(reasonerQueryFactory, query));
    }

    @Test
    @Repeat( times = repeat )
    public void whenSubstituionPresent_prioritiseSubbedRelationsOverNonSubbedOnes() {
        String queryString = "{" +
                "(someRole:$x, otherRole: $y) isa someRelation;" +
                "(someRole:$y, otherRole: $z) isa anotherRelation;" +
                "(someRole:$z, otherRole: $w) isa yetAnotherRelation;" +
                "$w id V123;" +
                "};";
        ReasonerQueryImpl query = reasonerQueryFactory.create(conjunction(queryString));
        ImmutableList<Atom> correctPlan = ImmutableList.of(
                getAtomOfType(query, "yetAnotherRelation"),
                getAtomOfType(query, "anotherRelation"),
                getAtomOfType(query, "someRelation")
        );
        checkOptimalAtomPlanProduced(query, correctPlan);
        checkPlanSanity(query);
    }

    @Test
    @Repeat( times = repeat )
    public void whenSubstitutionPresent_prioritiseSubbedResolvableRelationsOverNonSubbedNonResolvableOnes() {
        String queryString = "{" +
                "(someRole:$x, otherRole: $y) isa someRelation;" +
                "(someRole:$y, otherRole: $z) isa derivedRelation;" +
                "$z id V123;" +
                "};";
        ReasonerQueryImpl query = reasonerQueryFactory.create(conjunction(queryString));
        ImmutableList<Atom> correctPlan = ImmutableList.of(
                getAtomOfType(query, "derivedRelation"),
                getAtomOfType(query, "someRelation")
        );
        checkOptimalAtomPlanProduced(query, correctPlan);
        checkPlanSanity(query);
    }

    @Test
    @Repeat( times = repeat )
    public void whenMultipleSubsPresent_prioritiseMostSubbedRelations() {
        String queryString = "{" +
                "(someRole:$x, otherRole: $y) isa someRelation;" +
                "(someRole:$y, otherRole: $z) isa anotherRelation;" +
                "(someRole:$z, otherRole: $w) isa yetAnotherRelation;" +
                "$z id V123;" +
                "$w id V1232;" +
                "};";
        ReasonerQueryImpl query = reasonerQueryFactory.create(conjunction(queryString));
        ImmutableList<Atom> correctPlan = ImmutableList.of(
                getAtomOfType(query, "yetAnotherRelation"),
                getAtomOfType(query, "anotherRelation"),
                getAtomOfType(query, "someRelation")
        );
        checkOptimalAtomPlanProduced(query, correctPlan);
        checkPlanSanity(query);
    }

    @Ignore ("atm it is a degenerate case - we need to incorporate statistics into MST calculation")
    @Test
    @Repeat( times = repeat )
    public void whenOnlyAtomicQueriesPresent_prioritiseNonResolvableRelations_() {
        String queryString = "{" +
                "(someRole:$x, otherRole: $y) isa someRelation;" +
                "(someRole:$y, otherRole: $z) isa anotherDerivedRelation;" +
                "};";
        ReasonerQueryImpl query = reasonerQueryFactory.create(conjunction(queryString));
        ImmutableList<Atom> correctPlan = ImmutableList.of(
                getAtomOfType(query, "someRelation"),
                getAtomOfType(query, "anotherDerivedRelation")
        );
        checkOptimalAtomPlanProduced(query, correctPlan);
        checkPlanSanity(query);
    }

    @Ignore ("atm it is a degenerate case - we need to incorporate statistics into MST calculation")
    @Test
    @Repeat( times = repeat )
    public void whenSandwichedResolvableRelation_prioritiseNonResolvableRelations_() {
        String queryString = "{" +
                "(someRole:$x, otherRole: $y) isa someRelation;" +
                "(someRole:$y, otherRole: $z) isa anotherDerivedRelation;" +
                "(someRole:$z, otherRole: $w) isa yetAnotherRelation;" +
                "};";
        ReasonerQueryImpl query = reasonerQueryFactory.create(conjunction(queryString));
        checkPlanSanity(query);
        assertTrue(!new ResolutionPlan(query, traversalPlanFactory).plan().get(0).isRuleResolvable());
    }

    @Test
    @Repeat( times = repeat )
    public void whenSpecificResourcePresent_prioritiseSpecificResources(){
        String queryString = "{" +
                "(someRole:$x, otherRole: $y) isa someRelation;" +
                "(someRole:$y, otherRole: $z) isa anotherRelation;" +
                "(someRole:$z, otherRole: $w) isa yetAnotherRelation;" +
                "$w has resource 'test';" +
                "};";
        ReasonerQueryImpl query = reasonerQueryFactory.create(conjunction(queryString));
        ImmutableList<Atom> correctPlan = ImmutableList.of(
                getAtomOfType(query, "resource"),
                getAtomOfType(query, "yetAnotherRelation"),
                getAtomOfType(query, "anotherRelation"),
                getAtomOfType(query, "someRelation")
        );
        checkOptimalAtomPlanProduced(query, correctPlan);
        checkPlanSanity(query);
    }

    @Test
    @Repeat( times = repeat )
    public void whenSpecificResourcesAndRelationsWithGuardsPresent_prioritiseSpecificResources(){
        String queryString = "{" +
                "$x isa baseEntity;" +
                "(someRole:$x, otherRole: $y) isa derivedRelation;" +
                "$y isa someEntity;" +
                "$x has resource 'test';" +
                "};";
        ReasonerQueryImpl query = reasonerQueryFactory.create(conjunction(queryString));
        ImmutableList<Atom> correctPlan = ImmutableList.of(
                getAtomOfType(query, "resource"),
                getAtomOfType(query, "derivedRelation")
        );
        checkOptimalAtomPlanProduced(query, correctPlan);
        checkPlanSanity(query);
    }

    @Test
    @Repeat( times = repeat )
    public void whenSpecificAndNonspecificResourcesPresent_prioritiseSpecificResources(){

        String queryString = "{" +
                "(someRole:$x, otherRole: $y) isa someRelation;" +
                "(someRole:$y, otherRole: $z) isa anotherRelation;" +
                "(someRole:$z, otherRole: $w) isa yetAnotherRelation;" +
                "$x has anotherResource $r;" +
                "$w has resource 'test';" +
                "};";
        ReasonerQueryImpl query = reasonerQueryFactory.create(conjunction(queryString));
        ImmutableList<Atom> correctPlan = ImmutableList.of(
                getAtomOfType(query, "resource"),
                getAtomOfType(query, "yetAnotherRelation"),
                getAtomOfType(query, "anotherRelation"),
                getAtomOfType(query, "someRelation"),
                getAtomOfType(query, "anotherResource")
        );
        checkOptimalAtomPlanProduced(query, correctPlan);
        checkPlanSanity(query);
    }

    @Test
    @Repeat( times = repeat )
    public void whenNonSpecificResourcesPresent_doNotPrioritiseThem(){
        String queryString = "{" +
                "(someRole:$x, otherRole: $y) isa derivedRelation;" +
                "$x has resource $xr;" +
                "$y has resource $yr;" +
                "};";
        ReasonerQueryImpl query = reasonerQueryFactory.create(conjunction(queryString));
        assertEquals(new ResolutionPlan(query, traversalPlanFactory).plan().get(0), getAtomOfType(query, "derivedRelation"));
        checkPlanSanity(query);
    }

    /**
     * follows the following pattern:
     *
     * [$start/...] ($start, $link) - ($link, $anotherlink) - ($anotherlink, $end)* [$anotherlink/...]
     *
     */
    @Ignore ("should be fixed once we provide an estimate for inferred concepts count")
    @Test
    @Repeat( times = repeat )
    public void whenRelationLinkWithSubbedEndsAndRuleRelationInTheMiddle_exploitDBRelationsAndConnectivity(){
        String queryString = "{" +
                "$start id V123;" +
                "$end id V456;" +
                "(someRole: $link, otherRole: $start) isa someRelation;" +
                "(someRole: $link, otherRole: $anotherlink) isa derivedRelation;" +
                "(someRole: $anotherlink, otherRole: $end) isa anotherRelation;" +
                "$link isa someEntity;" +
                "$end isa someOtherEntity;" +
                "$anotherlink isa yetAnotherEntity;" +
                "};";
        ReasonerQueryImpl query = reasonerQueryFactory.create(conjunction(queryString));
        ResolutionQueryPlan resolutionQueryPlan =new ResolutionQueryPlan(reasonerQueryFactory, query);

        checkQueryPlanSanity(query);
        assertTrue(resolutionQueryPlan.queries().get(0).getAtoms(IdPredicate.class).findFirst().isPresent());
        checkAtomPlanSanity(query);
    }

    /**
     * follows the following pattern:
     *
     * [$start/...] ($start, $link) - ($link, $anotherlink) - ($anotherlink, $end)* [$anotherlink/...]
     *
     */
    @Test
    @Repeat( times = repeat )
    public void whenRelationLinkWithSubbedEndsAndRuleRelationAtEnd_exploitDBRelationsAndConnectivity(){
        String queryString = "{" +
                "$start id V123;" +
                "$end id V456;" +
                "(someRole: $link, otherRole: $start) isa someRelation;" +
                "(someRole: $anotherlink, otherRole: $link) isa anotherRelation;" +
                "(someRole: $end, otherRole: $anotherlink) isa derivedRelation;" +
                "$link isa someEntity;" +
                "$end isa someOtherEntity;" +
                "$anotherlink isa yetAnotherEntity;" +
                "};";
        ReasonerQueryImpl query = reasonerQueryFactory.create(conjunction(queryString));
        ResolutionQueryPlan resolutionQueryPlan =new ResolutionQueryPlan(reasonerQueryFactory, query);

        checkQueryPlanSanity(query);
        assertTrue(resolutionQueryPlan.queries().get(0).getAtoms(IdPredicate.class).findFirst().isPresent());
        assertTrue(!resolutionQueryPlan.queries().get(0).isAtomic());
        assertEquals(2, resolutionQueryPlan.queries().size());
        checkAtomPlanSanity(query);
    }

    /**
     * follows the following pattern
     *
     * [$start/...] ($start, $link) - ($link, $anotherlink)* - ($anotherlink, $end)*
     *              /                                                           |
     *        resource $res                                                  resource $res
     *    anotherResource 'someValue'
     */
    @Test
    @Repeat( times = repeat )
    public void whenRelationLinkWithEndsSharingAResource_exploitDBRelationsAndConnectivity(){
        String queryString = "{" +
                "$start id V123;" +
                "$start isa someEntity;" +
                "$start has anotherResource 'someValue';" +
                "$start has resource $res;" +
                "$end has resource $res;" +
                "(someRole: $link, otherRole: $start) isa someRelation;" +
                "(someRole: $link, otherRole: $anotherlink) isa derivedRelation;" +
                "(someRole: $anotherlink, otherRole: $end) isa anotherDerivedRelation;" +
                "};";
        ReasonerQueryImpl query = reasonerQueryFactory.create(conjunction(queryString));
        ResolutionQueryPlan resolutionQueryPlan = new ResolutionQueryPlan(reasonerQueryFactory, query);

        checkQueryPlanSanity(query);
        assertTrue(resolutionQueryPlan.queries().get(0).getAtoms(IdPredicate.class).findFirst().isPresent());
        System.out.println(resolutionQueryPlan);

        List<ReasonerQueryImpl> queries = resolutionQueryPlan.queries();
        //check that last two queries are the rule resolvable ones
        assertTrue(queries.get(queries.size()-1).isRuleResolvable());
        assertTrue(queries.get(queries.size()-2).isRuleResolvable());

        //TODO still might produce disconnected plans
        //checkAtomPlanSanity(query);
    }

    @Test
    @Repeat( times = repeat )
    public void whenIndirectTypeAtomsPresent_makeSureTheyAreNotLost(){
        String queryString = "{" +
                "$x isa baseEntity;" +
                "$y isa baseEntity;" +
                "(someRole:$x, otherRole: $xx) isa anotherRelation;$xx isa! $type;" +
                "(someRole:$y, otherRole: $yy) isa anotherRelation;$yy isa! $type;" +
                "$y != $x;" +
                "};";
        ReasonerQueryImpl query = reasonerQueryFactory.create(conjunction(queryString));
        checkAtomPlanComplete(query, new ResolutionPlan(query, traversalPlanFactory));
        checkQueryPlanComplete(query,new ResolutionQueryPlan(reasonerQueryFactory, query));
    }

    @Test
    @Repeat( times = repeat )
    public void whenResourcesWithSubstitutionsArePresent_makeSureOptimalOrderPicked() {
        Concept concept = tx.stream(Graql.match(var("x").isa("baseEntity")).get("x"))
                .map(ans -> ans.get("x")).findAny().orElse(null);
        String basePatternString =
                "(someRole:$x, otherRole: $y) isa someRelation;" +
                        "$x has resource 'this';" +
                        "$y has anotherResource 'that';";

        String xPatternString = "{" +
                "$x id " + concept.id() + ";" +
                basePatternString +
                "};";
        String yPatternString = "{" +
                "$y id " + concept.id() + ";" +
                basePatternString +
                "};";
        ReasonerQueryImpl queryX = reasonerQueryFactory.create(conjunction(xPatternString));
        ReasonerQueryImpl queryY = reasonerQueryFactory.create(conjunction(yPatternString));

        checkPlanSanity(queryX);
        checkPlanSanity(queryY);

        ImmutableList<Atom> xPlan = new ResolutionPlan(queryX, traversalPlanFactory).plan();
        ImmutableList<Atom> yPlan = new ResolutionPlan(queryY, traversalPlanFactory).plan();
        assertNotEquals(xPlan.get(0), getAtomOfType(queryX, "anotherResource"));
        assertNotEquals(yPlan.get(0), getAtomOfType(queryY, "resource"));
    }

    @Test
    @Repeat( times = repeat )
    public void whenRelationsWithSameTypesPresent_makeSureConnectednessPreserved(){
        String queryString = "{" +
                "(someRole:$x, otherRole: $y) isa someRelation;" +
                "(someRole:$y, otherRole: $z) isa anotherRelation;" +
                "(someRole:$z, otherRole: $w) isa someRelation;" +
                "(someRole:$w, otherRole: $u) isa anotherRelation;" +
                "(someRole:$u, otherRole: $v) isa someRelation;" +
                "};";
        ReasonerQueryImpl query = reasonerQueryFactory.create(conjunction(queryString));
        checkPlanSanity(query);
    }

    @Test
    @Repeat( times = repeat )
    public void whenRelationsWithSameTypesPresent_makeSureConnectednessPreserved_longerChain(){
        String queryString = "{" +
                "(someRole:$x, otherRole: $y) isa someRelation;" +
                "(someRole:$y, otherRole: $z) isa anotherRelation;" +
                "(someRole:$z, otherRole: $w) isa yetAnotherRelation;" +
                "(someRole:$w, otherRole: $u) isa someRelation;" +
                "(someRole:$u, otherRole: $v) isa anotherRelation;" +
                "(someRole:$v, otherRole: $q) isa yetAnotherRelation;"+
                "};";
        ReasonerQueryImpl query = reasonerQueryFactory.create(conjunction(queryString));
        checkPlanSanity(query);
    }

    /**
     follows the two-branch pattern
     /   (d, e) - (e, f)*
     (a, b)* - (b, c) - (c, d)*
     \   (d, g) - (g, h)*
     */
    @Test
    @Ignore ("flaky - need to reintroduce inferred concept counts")
    @Repeat( times = repeat )
    public void whenBranchedQueryChainsWithResolvableRelations_disconnectedPlansAreNotProduced(){

        String basePatternString =
                "($a, $b) isa derivedRelation;" +
                        "($b, $c) isa someRelation;" +
                        "($c, $d) isa anotherDerivedRelation;" +

                        "($d, $e) isa anotherRelation;" +
                        "($e, $f) isa derivedRelation;" +

                        "($d, $g) isa yetAnotherRelation;" +
                        "($g, $h) isa anotherDerivedRelation;";

        String queryString = "{" + basePatternString + "};";
        ReasonerQueryImpl query = reasonerQueryFactory.create(conjunction(queryString));
        checkPlanSanity(query);

        String attributedQueryString = "{" +
                "$a has resource 'someValue';" +
                basePatternString +
                "};";
        ReasonerQueryImpl attributedQuery = reasonerQueryFactory.create(conjunction(attributedQueryString));
        ResolutionPlan attributedResolutionPlan = new ResolutionPlan(attributedQuery, traversalPlanFactory);
        checkPlanSanity(attributedQuery);

        Atom efAtom = getAtomWithVariables(attributedQuery, Sets.newHashSet(new Variable("e"), new Variable("f")));
        Atom ghAtom = getAtomWithVariables(attributedQuery, Sets.newHashSet(new Variable("g"), new Variable("h")));

        ImmutableList<Atom> atomPlan = attributedResolutionPlan.plan();
        assertThat(atomPlan.get(atomPlan.size()-1), anyOf(is(efAtom), is(ghAtom)));
    }

    /**
     follows the two-branch pattern
     / (b, c)* - (c, d)
     (b, g) - (a, b)
     \ (b, e)* - (e, f)*
     */
    @Test
    @Repeat( times = repeat )
    public void whenBranchedQueryChainsWithResolvableRelations_disconnectedPlansAreNotProduced_anotherVariant(){
        String basePatternString =
                "($a, $b) isa someRelation;" +
                        "($b, $g) isa anotherRelation;" +

                        "($b, $c) isa derivedRelation;" +
                        "($c, $d) isa anotherDerivedRelation;" +

                        "($b, $e) isa derivedRelation;" +
                        "($e, $f) isa derivedRelation;";

        String queryString = "{" + basePatternString + "};";
        ReasonerQueryImpl query = reasonerQueryFactory.create(conjunction(queryString));
        checkPlanSanity(query);

        String attributedQueryString = "{" +
                "$g has resource 'someValue';" +
                basePatternString +
                "};";
        ReasonerQueryImpl attributedQuery = reasonerQueryFactory.create(conjunction(attributedQueryString));
        ResolutionPlan attributedResolutionPlan = new ResolutionPlan(attributedQuery, traversalPlanFactory);
        checkPlanSanity(attributedQuery);

        Atom efAtom = getAtomWithVariables(attributedQuery, Sets.newHashSet(new Variable("e"), new Variable("f")));
        Atom cdAtom = getAtomWithVariables(attributedQuery, Sets.newHashSet(new Variable("c"), new Variable("d")));

        ImmutableList<Atom> atomPlan = attributedResolutionPlan.plan();
        assertThat(atomPlan.get(atomPlan.size()-1), anyOf(is(efAtom), is(cdAtom)));
    }

    @Test
    @Repeat( times = repeat )
    public void whenQueryIsDisconnected_validPlanIsProduced(){
        String queryString = "{" +
                "$a isa baseEntity;" +
                "($a, $b) isa derivedRelation; $b isa someEntity;" +
                "$c isa baseEntity;" +
                "($c, $d) isa someRelation; $d isa someOtherEntity;" +
                "$e isa baseEntity;" +
                "($e, $f) isa anotherRelation; $f isa yetAnotherEntity;" +
                "};";

        ReasonerQueryImpl query = reasonerQueryFactory.create(conjunction(queryString));
        checkAtomPlanComplete(query, new ResolutionPlan(query, traversalPlanFactory));
        checkQueryPlanComplete(query,new ResolutionQueryPlan(reasonerQueryFactory, query));
    }

    @Test
    @Repeat( times = repeat )
    public void whenQueryIsNonTriviallyDisconnected_validPlanIsProduced(){
        String queryString = "{" +
                "$a isa baseEntity;" +
                "($a, $b) isa derivedRelation; $b isa someEntity;" +
                "($b, $c) isa someRelation; $c isa someEntity;" +
                "($c, $d) isa anotherRelation; $d isa someOtherEntity;" +

                "$e isa baseEntity;" +
                "($e, $f) isa someRelation; $f isa baseEntity;" +
                "($f, $g) isa anotherRelation; $g isa yetAnotherEntity;" +
                "($g, $h) isa derivedRelation; $h isa yetAnotherEntity;" +
                "};";

        ReasonerQueryImpl query = reasonerQueryFactory.create(conjunction(queryString));
        checkAtomPlanComplete(query, new ResolutionPlan(query, traversalPlanFactory));
        checkQueryPlanComplete(query,new ResolutionQueryPlan(reasonerQueryFactory, query));
    }

    /**
     * disconnected conjunction with specific concepts
     */
    @Test
    @Repeat( times = repeat )
    public void whenDisconnectedConjunctionWithSpecificConceptsPresent_itIsResolvedFirst(){
        String queryString = "{" +
                "$x isa someEntity;" +
                "$x has resource 'someValue';" +
                "$y isa someOtherEntity;" +
                "$y has anotherResource 'someOtherValue';" +

                "$x has derivedResource 'value';" +
                "$x has yetAnotherResource 'someValue';" +
                "};";

        ReasonerQueryImpl query = reasonerQueryFactory.create(conjunction(queryString));
        checkAtomPlanComplete(query, new ResolutionPlan(query, traversalPlanFactory));
        checkQueryPlanComplete(query,new ResolutionQueryPlan(reasonerQueryFactory, query));
    }

    /**
     * disconnected conjunction with ontological atom
     */
    @Test
    @Repeat( times = repeat )
    public void whenDisconnectedConjunctionWithOntologicalAtomPresent_itIsResolvedLast() {
        String queryString = "{" +
                "$x isa $type;" +
                "$type has resource;" +
                "$y isa someEntity;" +
                "$y has resource 'someValue';" +
                "($x, $y) isa derivedRelation;" +
                "};";
        ReasonerQueryImpl query = reasonerQueryFactory.create(conjunction(queryString));
        ResolutionPlan resolutionPlan = new ResolutionPlan(query, traversalPlanFactory);
        checkAtomPlanComplete(query, resolutionPlan);

        Atom resolvableIsa = getAtomWithVariables(query, Sets.newHashSet(new Variable("x"), new Variable("type")));
        assertThat(resolutionPlan.plan().get(2), is(resolvableIsa));

        checkQueryPlanComplete(query,new ResolutionQueryPlan(reasonerQueryFactory, query));
    }

    @Test
    @Repeat( times = repeat )
    public void whenPlanningForAFullyLinkedReifiedRelation_optimalStartingPointIsPicked() {
        Concept concept = tx.stream(Graql.match(var("x").isa("baseEntity")).get("x"))
                .map(ans -> ans.get("x")).findAny().orElse(null);
        String queryString = "{" +
                "$x id " + concept.id() + ";" +
                "($x, $link) isa someRelation;" +
                "($link, $y) isa anotherRelation;" +

                "$rel ($link, $z) isa derivedRelation;" +
                "$rel has resource 'value';" +
                "$rel has anotherResource $value;" +

                "};";

        ReasonerQueryImpl query = reasonerQueryFactory.create(conjunction(queryString));
        ResolutionPlan resolutionPlan = new ResolutionPlan(query, traversalPlanFactory);
        checkPlanSanity(query);
        checkQueryPlanComplete(query,new ResolutionQueryPlan(reasonerQueryFactory, query));

        Atom specificResource = getAtomOfType(query, "resource");
        Atom someRelation = getAtomOfType(query, "someRelation");
        assertThat(resolutionPlan.plan().get(0), Matchers.isOneOf(specificResource, someRelation));
    }

    @Test
    @Repeat( times = repeat )
    public void whenConjunctionChainWithASpecificAttribute_attributeResolvedBeforeConjunction(){
        String queryString = "{" +
                "$f has resource 'value'; $f isa someEntity;" +
                "($e, $f) isa derivedRelation; $e isa someOtherEntity;" +
                "($a, $b) isa someRelation; $a isa baseEntity;" +
                "($b, $c) isa anotherRelation; $b isa someEntity;" +
                "($c, $d) isa yetAnotherRelation; $c isa someOtherEntity;" +
                "($d, $e) isa someRelation; $d isa yetAnotherEntity;" +
                "};";
        ReasonerQueryImpl query = reasonerQueryFactory.create(conjunction(queryString));
        checkPlanSanity(query);
    }

    @Test
    public void whenEstimatingInferredCountOfAnInferredRelation_countIsDerivedFromMinimumPremiseCount(){
        Label someRelationLabel = Label.of("someRelation");
        Label anotherRelationLabel = Label.of("anotherRelation");
        Label derivedRelationLabel = Label.of("derivedRelation");
        Label anotherDerivedRelationLabel = Label.of("anotherDerivedRelation");
        TestTransactionProvider.TestTransaction testTx = ((TestTransactionProvider.TestTransaction)tx);
        assertEquals(
                tx.session().keyspaceStatistics().count(testTx.conceptManager(), someRelationLabel),
                NodesUtil.estimateInferredTypeCount(derivedRelationLabel, testTx.conceptManager(), planKeyspaceStatistics)
        );

        assertEquals(
                tx.session().keyspaceStatistics().count(testTx.conceptManager(), anotherRelationLabel),
                NodesUtil.estimateInferredTypeCount(anotherDerivedRelationLabel, testTx.conceptManager(), planKeyspaceStatistics)
        );
    }

    @Test
    public void whenEstimatingInferredCountOfAnInferredRecursiveRelation_countIsDerivedFromLeafType(){
        Label someRelationLabel = Label.of("someRelation");
        Label someRelationTransLabel = Label.of("someRelationTrans");
        TestTransactionProvider.TestTransaction testTx = ((TestTransactionProvider.TestTransaction)tx);
        assertEquals(
                tx.session().keyspaceStatistics().count(testTx.conceptManager(), someRelationLabel),
                NodesUtil.estimateInferredTypeCount(someRelationTransLabel, testTx.conceptManager(), planKeyspaceStatistics)
        );
    }

    private Atom getAtomWithVariables(ReasonerQuery query, Set<Variable> vars){
        return query.getAtoms(Atom.class).filter(at -> at.getVarNames().containsAll(vars)).findFirst().orElse(null);
    }

    private Atom getAtomOfType(ReasonerQueryImpl query, String typeString){
        Label label = Label.of(typeString);
        return query.getAtoms(Atom.class).filter(at -> at.getTypeLabel().equals(label)).findFirst().orElse(null);
    }

    private void checkPlanSanity(ReasonerQueryImpl query){
        checkAtomPlanSanity(query);
        checkQueryPlanSanity(query);
    }

    private void checkAtomPlanSanity(ReasonerQueryImpl query){
        ResolutionPlan resolutionPlan = new ResolutionPlan(query, traversalPlanFactory);
        checkAtomPlanComplete(query, resolutionPlan);
        checkAtomPlanConnected(resolutionPlan);
    }

    private void checkQueryPlanSanity(ReasonerQueryImpl query){
        ResolutionQueryPlan plan = new ResolutionQueryPlan(reasonerQueryFactory, query);
        checkQueryPlanComplete(query, plan);
        checkQueryPlanConnected(plan);
    }

    private void checkOptimalAtomPlanProduced(ReasonerQueryImpl query, ImmutableList<Atom> desiredAtomPlan) {
        ResolutionPlan resolutionPlan = new ResolutionPlan(query, traversalPlanFactory);
        ImmutableList<Atom> atomPlan = resolutionPlan.plan();
        assertEquals(desiredAtomPlan, atomPlan);
        checkAtomPlanComplete(query, resolutionPlan);
        checkAtomPlanConnected(resolutionPlan);
    }

    private void checkAtomPlanConnected(ResolutionPlan plan){
        ImmutableList<Atom> atomList = plan.plan();

        UnmodifiableIterator<Atom> iterator = atomList.iterator();
        Set<Variable> vars = new HashSet<>(iterator.next().getVarNames());
        while(iterator.hasNext()){
            Atom next = iterator.next();
            Set<Variable> varNames = next.getVarNames();
            assertFalse("Disconnected plan produced:\n" + plan, Sets.intersection(varNames, vars).isEmpty());
            vars.addAll(varNames);
        }
    }

    private void checkQueryPlanConnected(ResolutionQueryPlan plan){
        List<ReasonerQueryImpl> atomList = plan.queries();

        Iterator<ReasonerQueryImpl> iterator = atomList.iterator();
        Set<Variable> vars = new HashSet<>(iterator.next().getVarNames());
        while(iterator.hasNext()){
            ReasonerQueryImpl next = iterator.next();
            Set<Variable> varNames = next.getVarNames();
            boolean isDisconnected = Sets.intersection(varNames, vars).isEmpty();
            assertFalse("Disconnected query plan produced:\n" + plan, isDisconnected);
            vars.addAll(varNames);
        }
    }

    private void checkAtomPlanComplete(ReasonerQueryImpl query, ResolutionPlan plan){
        assertEquals(query.selectAtoms().collect(toSet()), Sets.newHashSet(plan.plan()) );
    }

    private void checkQueryPlanComplete(ReasonerQueryImpl query, ResolutionQueryPlan plan){
        assertEquals(query.selectAtoms().collect(toSet()), plan.queries().stream().flatMap(ReasonerQueryImpl::selectAtoms).collect(toSet()));
    }

    private Conjunction<Statement> conjunction(String patternString){
        Set<Statement> vars = Graql.parsePattern(patternString)
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Graql.and(vars);
    }

    static class RepeatRule implements TestRule {

        private static class RepeatStatement extends org.junit.runners.model.Statement {

            private final int times;
            private final org.junit.runners.model.Statement statement;

            private RepeatStatement(int times, org.junit.runners.model.Statement statement) {
                this.times = times;
                this.statement = statement;
            }

            @Override
            public void evaluate() throws Throwable {
                for( int i = 0; i < times; i++ ) {
                    statement.evaluate();
                }
            }
        }

        @Override
        public org.junit.runners.model.Statement apply(org.junit.runners.model.Statement statement, Description description) {
            org.junit.runners.model.Statement result = statement;
            Repeat repeat = description.getAnnotation(Repeat.class);
            if( repeat != null ) {
                int times = repeat.times();
                result = new RepeatStatement(times, statement);
            }
            return result;
        }
    }
}

@Retention( RetentionPolicy.RUNTIME )
@Target(METHOD)
@interface Repeat {
    int times();
}

