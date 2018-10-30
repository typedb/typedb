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

package ai.grakn.graql.internal.reasoner;

import ai.grakn.GraknTx;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Label;
import ai.grakn.concept.Type;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.plan.ResolutionPlan;
import ai.grakn.graql.internal.reasoner.plan.ResolutionQueryPlan;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.test.rule.SampleKBContext;

import ai.grakn.util.Repeat;
import ai.grakn.util.RepeatRule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.collect.UnmodifiableIterator;

import java.util.Iterator;
import java.util.List;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static ai.grakn.graql.Graql.var;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class ResolutionPlanTest {

    private static final int repeat = 10;

    @Rule
    public RepeatRule repeatRule = new RepeatRule();

    @ClassRule
    public static final SampleKBContext testContext = SampleKBContext.load("resolution-plan-test.gql");

    @Test
    @Repeat( times = repeat )
    public void makeSureDisconnectedIndexedQueriesProduceCompletePlan_indexedResource() {
        EmbeddedGraknTx<?> testTx = testContext.tx();
        String queryString = "{" +
                "$x isa someEntity;" +
                "$y isa resource;$y 'value';" +
                "$z isa relation;" +
                "}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(queryString, testTx), testTx);
        new ResolutionPlan(query);
    }

    @Test
    @Repeat( times = repeat )
    public void makeSureDisconnectedIndexedQueriesProduceCompletePlan_indexedEntity() {
        EmbeddedGraknTx<?> testTx = testContext.tx();
        String queryString = "{" +
                "$x isa someEntity;$x id 'V123';" +
                "$y isa resource;" +
                "$z isa relation;" +
                "}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(queryString, testTx), testTx);
        new ResolutionPlan(query);
    }

    @Test
    @Repeat( times = repeat )
    public void prioritiseSubbedRelationsOverNonSubbedOnes() {
        EmbeddedGraknTx<?> testTx = testContext.tx();
        String queryString = "{" +
                "(someRole:$x, otherRole: $y) isa relation;" +
                "(someRole:$y, otherRole: $z) isa anotherRelation;" +
                "(someRole:$z, otherRole: $w) isa yetAnotherRelation;" +
                "$w id 'sampleId';" +
                "}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(queryString, testTx), testTx);
        ImmutableList<Atom> correctPlan = ImmutableList.of(
                getAtomOfType(query, "yetAnotherRelation", testTx),
                getAtomOfType(query, "anotherRelation", testTx),
                getAtomOfType(query, "relation", testTx)
        );
        checkOptimalAtomPlanProduced(query, correctPlan);
        checkPlanSanity(query);
    }

    @Test
    @Repeat( times = repeat )
    public void prioritiseSubbedResolvableRelationsOverNonSubbedNonResolvableOnes() {
        EmbeddedGraknTx<?> testTx = testContext.tx();
        String queryString = "{" +
                "(someRole:$x, otherRole: $y) isa relation;" +
                "(someRole:$y, otherRole: $z) isa derivedRelation;" +
                "$z id 'sampleId';" +
                "}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(queryString, testTx), testTx);
        ImmutableList<Atom> correctPlan = ImmutableList.of(
                getAtomOfType(query, "derivedRelation", testTx),
                getAtomOfType(query, "relation", testTx)
        );
        checkOptimalAtomPlanProduced(query, correctPlan);
        checkPlanSanity(query);
    }

    @Test
    @Repeat( times = repeat )
    public void prioritiseMostSubbedRelations() {
        EmbeddedGraknTx<?> testTx = testContext.tx();
        String queryString = "{" +
                "(someRole:$x, otherRole: $y) isa relation;" +
                "(someRole:$y, otherRole: $z) isa anotherRelation;" +
                "(someRole:$z, otherRole: $w) isa yetAnotherRelation;" +
                "$z id 'sampleId';" +
                "$w id 'sampleId2';" +
                "}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(queryString, testTx), testTx);
        ImmutableList<Atom> correctPlan = ImmutableList.of(
                getAtomOfType(query, "yetAnotherRelation", testTx),
                getAtomOfType(query, "anotherRelation", testTx),
                getAtomOfType(query, "relation", testTx)
        );
        checkOptimalAtomPlanProduced(query, correctPlan);
        checkPlanSanity(query);
    }

    //TODO refined plan should solve this
    @Ignore
    @Test
    @Repeat( times = repeat )
    public void prioritiseNonResolvableRelations_OnlyAtomicQueriesPresent() {
        EmbeddedGraknTx<?> testTx = testContext.tx();
        String queryString = "{" +
                "(someRole:$x, otherRole: $y) isa relation;" +
                "(someRole:$y, otherRole: $z) isa derivedRelation;" +
                "}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(queryString, testTx), testTx);
        ImmutableList<Atom> correctPlan = ImmutableList.of(
                getAtomOfType(query, "relation", testTx),
                getAtomOfType(query, "derivedRelation", testTx)
        );
        checkOptimalAtomPlanProduced(query, correctPlan);
        checkPlanSanity(query);
    }

    //TODO refined plan should solve this
    @Ignore
    @Test
    @Repeat( times = repeat )
    public void prioritiseNonResolvableRelations_SandwichedResolvableRelation() {
        EmbeddedGraknTx<?> testTx = testContext.tx();
        String queryString = "{" +
                "(someRole:$x, otherRole: $y) isa relation;" +
                "(someRole:$y, otherRole: $z) isa derivedRelation;" +
                "(someRole:$z, otherRole: $w) isa yetAnotherRelation;" +
                "}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(queryString, testTx), testTx);
        checkPlanSanity(query);
        assertTrue(!new ResolutionPlan(query).plan().get(0).isRuleResolvable());
    }

    @Test
    @Repeat( times = repeat )
    public void prioritiseSpecificResourcesOverRelations(){
        EmbeddedGraknTx<?> testTx = testContext.tx();
        String queryString = "{" +
                "(someRole:$x, otherRole: $y) isa relation;" +
                "(someRole:$y, otherRole: $z) isa anotherRelation;" +
                "(someRole:$z, otherRole: $w) isa yetAnotherRelation;" +
                "$w has resource 'test';" +
                "}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(queryString, testTx), testTx);
        ImmutableList<Atom> correctPlan = ImmutableList.of(
                getAtomOfType(query, "resource", testTx),
                getAtomOfType(query, "yetAnotherRelation", testTx),
                getAtomOfType(query, "anotherRelation", testTx),
                getAtomOfType(query, "relation", testTx)
        );
        checkOptimalAtomPlanProduced(query, correctPlan);
        checkPlanSanity(query);
    }

    @Test
    @Repeat( times = repeat )
    public void prioritiseSpecificResourcesOverResolvableRelationsWithGuards(){
        EmbeddedGraknTx<?> testTx = testContext.tx();
        String queryString = "{" +
                "$x isa baseEntity;" +
                "(someRole:$x, otherRole: $y) isa derivedRelation;" +
                "$y isa someEntity;" +
                "$x has resource 'test';" +
                "}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(queryString, testTx), testTx);
        ImmutableList<Atom> correctPlan = ImmutableList.of(
                getAtomOfType(query, "resource", testTx),
                getAtomOfType(query, "derivedRelation", testTx)
        );
        checkOptimalAtomPlanProduced(query, correctPlan);
        checkPlanSanity(query);
    }

    @Test
    @Repeat( times = repeat )
    public void prioritiseSpecificResourcesOverNonSpecific(){
        EmbeddedGraknTx<?> testTx = testContext.tx();
        String queryString = "{" +
                "(someRole:$x, otherRole: $y) isa relation;" +
                "(someRole:$y, otherRole: $z) isa anotherRelation;" +
                "(someRole:$z, otherRole: $w) isa yetAnotherRelation;" +
                "$x has anotherResource $r;" +
                "$w has resource 'test';" +
                "}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(queryString, testTx), testTx);
        ImmutableList<Atom> correctPlan = ImmutableList.of(
                getAtomOfType(query, "resource", testTx),
                getAtomOfType(query, "yetAnotherRelation", testTx),
                getAtomOfType(query, "anotherRelation", testTx),
                getAtomOfType(query, "relation", testTx),
                getAtomOfType(query, "anotherResource", testTx)
        );
        checkOptimalAtomPlanProduced(query, correctPlan);
        checkPlanSanity(query);
    }

    @Test
    @Repeat( times = repeat )
    public void doNotPrioritiseNonSpecificResources(){
        EmbeddedGraknTx<?> testTx = testContext.tx();
        String queryString = "{" +
                "(someRole:$x, otherRole: $y) isa derivedRelation;" +
                "$x has resource $xr;" +
                "$y has resource $yr;" +
                "}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(queryString, testTx), testTx);
        assertEquals(new ResolutionPlan(query).plan().get(0), getAtomOfType(query, "derivedRelation", testTx));
        checkPlanSanity(query);
    }

    /**
     * follows the following pattern:
     *
     * [$start/...] ($start, $link) - ($link, $anotherlink) - ($anotherlink, $end)* [$anotherlink/...]
     *
     */
    //TODO refined plan should solve this
    @Ignore
    @Test
    public void exploitDBRelationsAndConnectivity_relationLinkWithSubbedEndsAndRuleRelationInTheMiddle(){
        EmbeddedGraknTx<?> testTx = testContext.tx();
        String queryString = "{" +
                "$start id 'someSampleId';" +
                "$end id 'anotherSampleId';" +
                "(someRole: $link, otherRole: $start) isa relation;" +
                "(someRole: $link, otherRole: $anotherlink) isa derivedRelation;" +
                "(someRole: $anotherlink, otherRole: $end) isa anotherRelation;" +
                "$link isa someEntity;" +
                "$end isa someOtherEntity;" +
                "$anotherlink isa yetAnotherEntity;" +
                "}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(queryString, testTx), testTx);
        ResolutionQueryPlan resolutionQueryPlan = new ResolutionQueryPlan(query);

        checkQueryPlanSanity(query);
        assertTrue(resolutionQueryPlan.queries().get(0).getAtoms(IdPredicate.class).findFirst().isPresent());
        assertEquals(2, resolutionQueryPlan.queries().size());
        //TODO still might produce disconnected plans
        checkAtomPlanSanity(query);
    }

    /**
     * follows the following pattern:
     *
     * [$start/...] ($start, $link) - ($link, $anotherlink) - ($anotherlink, $end)* [$anotherlink/...]
     *
     */
    //TODO refined plan should solve this
    //@Ignore
    @Test
    public void exploitDBRelationsAndConnectivity_relationLinkWithSubbedEndsAndRuleRelationAtEnd(){
        EmbeddedGraknTx<?> testTx = testContext.tx();
        String queryString = "{" +
                "$start id 'someSampleId';" +
                "$end id 'anotherSampleId';" +
                "(someRole: $link, otherRole: $start) isa relation;" +
                "(someRole: $anotherlink, otherRole: $link) isa anotherRelation;" +
                "(someRole: $end, otherRole: $anotherlink) isa derivedRelation;" +
                "$link isa someEntity;" +
                "$end isa someOtherEntity;" +
                "$anotherlink isa yetAnotherEntity;" +
                "}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(queryString, testTx), testTx);
        ResolutionQueryPlan resolutionQueryPlan = new ResolutionQueryPlan(query);

        checkQueryPlanSanity(query);
        assertTrue(resolutionQueryPlan.queries().get(0).getAtoms(IdPredicate.class).findFirst().isPresent());
        assertTrue(!resolutionQueryPlan.queries().get(0).isAtomic());
        assertEquals(2, resolutionQueryPlan.queries().size());
        //TODO still might produce disconnected plans
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
    //TODO flaky!
    @Ignore
    @Test
    public void exploitDBRelationsAndConnectivity_relationLinkWithEndsSharingAResource(){
        EmbeddedGraknTx<?> testTx = testContext.tx();
        String queryString = "{" +
                "$start id 'sampleId';" +
                "$start isa someEntity;" +
                "$start has anotherResource 'someValue';" +
                "$start has resource $res;" +
                "$end has resource $res;" +
                "(someRole: $link, otherRole: $start) isa relation;" +
                "(someRole: $link, otherRole: $anotherlink) isa derivedRelation;" +
                "(someRole: $anotherlink, otherRole: $end) isa anotherDerivedRelation;" +
                "}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(queryString, testTx), testTx);
        ResolutionQueryPlan resolutionQueryPlan = new ResolutionQueryPlan(query);

        checkQueryPlanSanity(query);
        assertTrue(resolutionQueryPlan.queries().get(0).getAtoms(IdPredicate.class).findFirst().isPresent());
        assertTrue(!resolutionQueryPlan.queries().get(0).isAtomic());
        //TODO still might produce disconnected plans
        //checkAtomPlanSanity(query);
    }

    @Test
    @Repeat( times = repeat )
    public void makeSureIndirectTypeAtomsAreNotLostWhenPlanning(){
        EmbeddedGraknTx<?> testTx = testContext.tx();
        String queryString = "{" +
                "$x isa baseEntity;" +
                "$y isa baseEntity;" +
                "(someRole:$x, otherRole: $xx) isa anotherRelation;$xx isa! $type;" +
                "(someRole:$y, otherRole: $yy) isa anotherRelation;$yy isa! $type;" +
                "$y != $x;" +
                "}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(queryString, testTx), testTx);
        checkAtomPlanComplete(query, new ResolutionPlan(query));
        checkQueryPlanComplete(query, new ResolutionQueryPlan(query));
    }

    @Test
    @Repeat( times = repeat )
    public void makeSureOptimalOrderPickedWhenResourcesWithSubstitutionsArePresent() {
        EmbeddedGraknTx<?> testTx = testContext.tx();
        Concept concept = testTx.graql().match(var("x").isa("baseEntity")).get("x")
                .stream().map(ans -> ans.get("x")).findAny().orElse(null);
        String basePatternString =
                "(someRole:$x, otherRole: $y) isa relation;" +
                        "$x has resource 'this';" +
                        "$y has anotherResource 'that';";

        String xPatternString = "{" +
                "$x id '" + concept.id() + "';" +
                basePatternString +
                "}";
        String yPatternString = "{" +
                "$y id '" + concept.id() + "';" +
                basePatternString +
                "}";
        ReasonerQueryImpl queryX = ReasonerQueries.create(conjunction(xPatternString, testTx), testTx);
        ReasonerQueryImpl queryY = ReasonerQueries.create(conjunction(yPatternString, testTx), testTx);

        checkPlanSanity(queryX);
        checkPlanSanity(queryY);

        assertNotEquals(new ResolutionPlan(queryX).plan().get(0), getAtomOfType(queryX, "anotherResource", testTx));
        assertNotEquals(new ResolutionPlan(queryY).plan().get(0), getAtomOfType(queryX, "resource", testTx));
    }

    @Test
    @Repeat( times = repeat )
    public void makeSureConnectednessPreservedWhenRelationsWithSameTypesPresent(){
        EmbeddedGraknTx<?> testTx = testContext.tx();
        String queryString = "{" +
                "(someRole:$x, otherRole: $y) isa relation;" +
                "(someRole:$y, otherRole: $z) isa anotherRelation;" +
                "(someRole:$z, otherRole: $w) isa relation;" +
                "(someRole:$w, otherRole: $u) isa anotherRelation;" +
                "(someRole:$u, otherRole: $v) isa relation;" +
                "}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(queryString, testTx), testTx);
        checkPlanSanity(query);
    }

    @Test
    @Repeat( times = repeat )
    public void makeSureConnectednessPreservedWhenRelationsWithSameTypesPresent_longerChain(){
        EmbeddedGraknTx<?> testTx = testContext.tx();
        String queryString = "{" +
                "(someRole:$x, otherRole: $y) isa relation;" +
                "(someRole:$y, otherRole: $z) isa anotherRelation;" +
                "(someRole:$z, otherRole: $w) isa yetAnotherRelation;" +
                "(someRole:$w, otherRole: $u) isa relation;" +
                "(someRole:$u, otherRole: $v) isa anotherRelation;" +
                "(someRole:$v, otherRole: $q) isa yetAnotherRelation;"+
                "}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(queryString, testTx), testTx);
        checkPlanSanity(query);
    }

    /**
     follows the two-branch pattern
                                /   (d, e) - (e, f)*
     (a, b)* - (b, c) - (c, d)*
                                \   (d, g) - (g, h)*
     */
    @Test
    @Repeat( times = repeat )
    public void makeSureBranchedQueryChainsWithResolvableRelationsDoNotProduceDisconnectedPlans(){
        EmbeddedGraknTx<?> testTx = testContext.tx();

        String basePatternString =
                "($a, $b) isa derivedRelation;" +
                        "($b, $c) isa relation;" +
                        "($c, $d) isa anotherDerivedRelation;" +

                        "($d, $e) isa anotherRelation;" +
                        "($e, $f) isa derivedRelation;" +

                        "($d, $g) isa yetAnotherRelation;" +
                        "($g, $h) isa anotherDerivedRelation;";

        String queryString = "{" + basePatternString + "}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(queryString, testTx), testTx);
        checkPlanSanity(query);

        String attributedQueryString = "{" +
                "$a has resource 'someValue';" +
                basePatternString +
                "}";
        ReasonerQueryImpl attributedQuery = ReasonerQueries.create(conjunction(attributedQueryString, testTx), testTx);
        ResolutionPlan attributedResolutionPlan = new ResolutionPlan(attributedQuery);
        checkPlanSanity(attributedQuery);

        Atom efAtom = getAtomWithVariables(attributedQuery, Sets.newHashSet(var("e"), var("f")));
        Atom ghAtom = getAtomWithVariables(attributedQuery, Sets.newHashSet(var("g"), var("h")));

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
    public void makeSureBranchedQueryChainsWithResolvableRelationsDoNotProduceDisconnectedPlans_anotherVariant(){
        EmbeddedGraknTx<?> testTx = testContext.tx();

        String basePatternString =
                "($a, $b) isa relation;" +
                        "($b, $g) isa anotherRelation;" +

                        "($b, $c) isa derivedRelation;" +
                        "($c, $d) isa anotherDerivedRelation;" +

                        "($b, $e) isa derivedRelation;" +
                        "($e, $f) isa derivedRelation;";

        String queryString = "{" + basePatternString + "}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(queryString, testTx), testTx);
        checkPlanSanity(query);

        String attributedQueryString = "{" +
                "$g has resource 'someValue';" +
                basePatternString +
                "}";
        ReasonerQueryImpl attributedQuery = ReasonerQueries.create(conjunction(attributedQueryString, testTx), testTx);
        ResolutionPlan attributedResolutionPlan = new ResolutionPlan(attributedQuery);
        checkPlanSanity(attributedQuery);

        Atom efAtom = getAtomWithVariables(attributedQuery, Sets.newHashSet(var("e"), var("f")));
        Atom cdAtom = getAtomWithVariables(attributedQuery, Sets.newHashSet(var("c"), var("d")));

        ImmutableList<Atom> atomPlan = attributedResolutionPlan.plan();
        assertThat(atomPlan.get(atomPlan.size()-1), anyOf(is(efAtom), is(cdAtom)));
    }

    @Test
    @Repeat( times = repeat )
    public void makeSureDisconnectedQueryProducesValidPlan(){
        EmbeddedGraknTx<?> testTx = testContext.tx();
        String queryString = "{" +
                "$a isa baseEntity;" +
                "($a, $b) isa derivedRelation; $b isa someEntity;" +
                "$c isa baseEntity;" +
                "($c, $d) isa relation; $d isa someOtherEntity;" +
                "$e isa baseEntity;" +
                "($e, $f) isa anotherRelation; $f isa yetAnotherEntity;" +
                "}";

        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(queryString, testTx), testTx);
        checkAtomPlanComplete(query, new ResolutionPlan(query));
        checkQueryPlanComplete(query, new ResolutionQueryPlan(query));
    }

    @Test
    @Repeat( times = repeat )
    public void makeSureNonTrivialDisconnectedQueryProducesValidPlan(){
        EmbeddedGraknTx<?> testTx = testContext.tx();
        String queryString = "{" +
                "$a isa baseEntity;" +
                "($a, $b) isa derivedRelation; $b isa someEntity;" +
                "($b, $c) isa relation; $c isa someEntity;" +
                "($c, $d) isa anotherRelation; $d isa someOtherEntity;" +

                "$e isa baseEntity;" +
                "($e, $f) isa relation; $f isa baseEntity;" +
                "($f, $g) isa anotherRelation; $g isa yetAnotherEntity;" +
                "($g, $h) isa derivedRelation; $h isa yetAnotherEntity;" +
                "}";

        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(queryString, testTx), testTx);
        checkAtomPlanComplete(query, new ResolutionPlan(query));
        checkQueryPlanComplete(query, new ResolutionQueryPlan(query));
    }

    /**
     * disconnected conjunction with specific concepts
     */
    @Test
    @Repeat( times = repeat )
    public void makeSureDisconnectedConjunctionWithSpecificConceptsResolvedFirst(){
        EmbeddedGraknTx<?> testTx = testContext.tx();
        String queryString = "{" +
                "$x isa someEntity;" +
                "$x has resource 'someValue';" +
                "$y isa someOtherEntity;" +
                "$y has anotherResource 'someOtherValue';" +

                "$x has derivedResource 'value';" +
                "$x has yetAnotherResource 'someValue';" +
                "}";

        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(queryString, testTx), testTx);
        checkAtomPlanComplete(query, new ResolutionPlan(query));
        checkQueryPlanComplete(query, new ResolutionQueryPlan(query));
    }

    /**
     * disconnected conjunction with ontological atom
     */
    @Test
    @Repeat( times = repeat )
    public void makeSureDisconnectedConjunctionWithOntologicalAtomResolvedFirst() {
        EmbeddedGraknTx<?> testTx = testContext.tx();
        String queryString = "{" +
                "$x isa $type;" +
                "$type has resource;" +
                "$y isa someEntity;" +
                "$y has resource 'someValue';" +
                "($x, $y) isa derivedRelation;" +
                "}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(queryString, testTx), testTx);
        ResolutionPlan resolutionPlan = new ResolutionPlan(query);
        checkAtomPlanComplete(query, resolutionPlan);

        Atom resolvableIsa = getAtomWithVariables(query, Sets.newHashSet(var("x"), var("type")));
        assertThat(resolutionPlan.plan().get(3), is(resolvableIsa));

        checkQueryPlanComplete(query, new ResolutionQueryPlan(query));
    }

    @Test
    @Repeat( times = repeat )
    public void makeSureAttributeResolvedBeforeConjunction(){
        EmbeddedGraknTx<?> testTx = testContext.tx();
        String queryString = "{" +
                "$f has resource 'value'; $f isa someEntity;" +
                "($e, $f) isa derivedRelation; $e isa someOtherEntity;" +
                "($a, $b) isa relation; $a isa baseEntity;" +
                "($b, $c) isa anotherRelation; $b isa someEntity;" +
                "($c, $d) isa yetAnotherRelation; $c isa someOtherEntity;" +
                "($d, $e) isa relation; $d isa yetAnotherEntity;" +
                "}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(queryString, testTx), testTx);
        checkPlanSanity(query);
        //todo
    }

    private Atom getAtomWithVariables(ReasonerQuery query, Set<Var> vars){
        return query.getAtoms(Atom.class).filter(at -> at.getVarNames().containsAll(vars)).findFirst().orElse(null);
    }

    private Atom getAtomOfType(ReasonerQueryImpl query, String typeString, GraknTx tx){
        Type type = tx.getType(Label.of(typeString));
        return query.getAtoms(Atom.class).filter(at -> at.getTypeId().equals(type.id())).findFirst().orElse(null);
    }

    private void checkPlanSanity(ReasonerQueryImpl query){
        checkAtomPlanSanity(query);
        checkQueryPlanSanity(query);
    }

    private void checkAtomPlanSanity(ReasonerQueryImpl query){
        ResolutionPlan resolutionPlan = new ResolutionPlan(query);
        checkAtomPlanComplete(query, resolutionPlan);
        checkAtomPlanConnected(resolutionPlan);
    }

    private void checkQueryPlanSanity(ReasonerQueryImpl query){
        ResolutionQueryPlan plan = new ResolutionQueryPlan(query);
        checkQueryPlanComplete(query, plan);
        checkQueryPlanConnected(plan);
    }

    private void checkOptimalAtomPlanProduced(ReasonerQueryImpl query, ImmutableList<Atom> desiredAtomPlan) {
        ResolutionPlan resolutionPlan = new ResolutionPlan(query);
        ImmutableList<Atom> atomPlan = resolutionPlan.plan();
        assertEquals(desiredAtomPlan, atomPlan);
        checkAtomPlanComplete(query, resolutionPlan);
        checkAtomPlanConnected(resolutionPlan);
    }

    private void checkAtomPlanConnected(ResolutionPlan plan){
        ImmutableList<Atom> atomList = plan.plan();

        UnmodifiableIterator<Atom> iterator = atomList.iterator();
        Set<Var> vars = new HashSet<>(iterator.next().getVarNames());
        while(iterator.hasNext()){
            Atom next = iterator.next();
            Set<Var> varNames = next.getVarNames();
            assertTrue("Disconnected plan produced:\n" + plan, !Sets.intersection(varNames, vars).isEmpty());
            vars.addAll(varNames);
        }
    }

    private void checkQueryPlanConnected(ResolutionQueryPlan plan){
        List<ReasonerQueryImpl> atomList = plan.queries();

        Iterator<ReasonerQueryImpl> iterator = atomList.iterator();
        Set<Var> vars = new HashSet<>(iterator.next().getVarNames());
        while(iterator.hasNext()){
            ReasonerQueryImpl next = iterator.next();
            Set<Var> varNames = next.getVarNames();
            boolean isDisconnected = Sets.intersection(varNames, vars).isEmpty();
            assertTrue("Disconnected query plan produced:\n" + plan, !isDisconnected);
            vars.addAll(varNames);
        }
    }

    private void checkAtomPlanComplete(ReasonerQueryImpl query, ResolutionPlan plan){
        assertEquals(query.selectAtoms().collect(toSet()), Sets.newHashSet(plan.plan()) );
    }

    private void checkQueryPlanComplete(ReasonerQueryImpl query, ResolutionQueryPlan plan){
        assertEquals(query.selectAtoms().collect(toSet()), plan.queries().stream().flatMap(ReasonerQueryImpl::selectAtoms).collect(toSet()));
    }

    private Conjunction<VarPatternAdmin> conjunction(String patternString, GraknTx graph){
        Set<VarPatternAdmin> vars = graph.graql().parser().parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }
}
