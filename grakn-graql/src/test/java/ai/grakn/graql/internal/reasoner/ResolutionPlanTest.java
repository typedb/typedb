/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

package ai.grakn.graql.internal.reasoner;

import ai.grakn.GraknTx;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Label;
import ai.grakn.concept.Type;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.plan.ResolutionPlan;
import ai.grakn.graql.internal.reasoner.plan.ResolutionQueryPlan;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.test.rule.SampleKBContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.collect.UnmodifiableIterator;
import java.util.Iterator;
import java.util.List;
import org.junit.ClassRule;
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

    @ClassRule
    public static final SampleKBContext testContext = SampleKBContext.load("resolution-plan-test.gql");


    @Test
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
                getAtom(query, "yetAnotherRelation", testTx),
                getAtom(query, "anotherRelation", testTx),
                getAtom(query, "relation", testTx)
        );
        checkOptimalAtomPlanProduced(query, correctPlan);
        checkPlanSanity(query);
    }

    @Test
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
                getAtom(query, "yetAnotherRelation", testTx),
                getAtom(query, "anotherRelation", testTx),
                getAtom(query, "relation", testTx)
        );
        checkOptimalAtomPlanProduced(query, correctPlan);
        checkPlanSanity(query);
    }

    @Test
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
                getAtom(query, "resource", testTx),
                getAtom(query, "yetAnotherRelation", testTx),
                getAtom(query, "anotherRelation", testTx),
                getAtom(query, "relation", testTx)
        );
        checkOptimalAtomPlanProduced(query, correctPlan);
        checkPlanSanity(query);
    }

    @Test
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
                getAtom(query, "resource", testTx),
                getAtom(query, "yetAnotherRelation", testTx),
                getAtom(query, "anotherRelation", testTx),
                getAtom(query, "relation", testTx),
                getAtom(query, "anotherResource", testTx)
        );
        checkOptimalAtomPlanProduced(query, correctPlan);
        checkPlanSanity(query);
    }

    @Test
    public void doNotPrioritiseNonSpecificResources(){
        EmbeddedGraknTx<?> testTx = testContext.tx();
        String queryString = "{" +
                "(someRole:$x, otherRole: $y) isa derivedRelation;" +
                "$x has resource $xr;" +
                "$y has resource $yr;" +
                "}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(queryString, testTx), testTx);
        assertEquals(new ResolutionPlan(query).plan().get(0), getAtom(query, "derivedRelation", testTx));
        checkPlanSanity(query);
    }

    @Test
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
    public void makeSureOptimalOrderPickedWhenResourcesWithSubstitutionsArePresent() {
        EmbeddedGraknTx<?> testTx = testContext.tx();
        Concept concept = testTx.graql().match(var("x").isa("baseEntity")).get("x")
                .stream().map(ans -> ans.get("x")).findAny().orElse(null);
        String basePatternString =
                "(someRole:$x, otherRole: $y) isa relation;" +
                        "$x has resource 'this';" +
                        "$y has anotherResource 'that';";

        String xPatternString = "{" +
                "$x id '" + concept.getId() + "';" +
                basePatternString +
                "}";
        String yPatternString = "{" +
                "$y id '" + concept.getId() + "';" +
                basePatternString +
                "}";
        ReasonerQueryImpl queryX = ReasonerQueries.create(conjunction(xPatternString, testTx), testTx);
        ReasonerQueryImpl queryY = ReasonerQueries.create(conjunction(yPatternString, testTx), testTx);

        checkPlanSanity(queryX);
        checkPlanSanity(queryY);

        assertNotEquals(new ResolutionPlan(queryX).plan().get(0), getAtom(queryX, "anotherResource", testTx));
        assertNotEquals(new ResolutionPlan(queryY).plan().get(0), getAtom(queryX, "resource", testTx));
    }

    @Test
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

    @Test
    public void makeSureLongQueryChainsWithResolvableRelationsDoNotProduceDisconnectedPlans(){
        EmbeddedGraknTx<?> testTx = testContext.tx();
        /*
        follows two-branch pattern
                                    /   (d, e) - (e, f)*
        (a, b)* - (b, c) - (c, d)*
                                    \   (d, g) - (g, h)*
         */
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

        Atom efAtom = attributedQuery.getAtoms(Atom.class).filter(at -> at.getVarNames().containsAll(Sets.newHashSet(var("e"), var("f")))).findFirst().orElse(null);
        Atom ghAtom = attributedQuery.getAtoms(Atom.class).filter(at -> at.getVarNames().containsAll(Sets.newHashSet(var("g"), var("h")))).findFirst().orElse(null);

        ImmutableList<Atom> atomPlan = attributedResolutionPlan.plan();
        assertThat(atomPlan.get(atomPlan.size()-1), anyOf(is(efAtom), is(ghAtom)));
    }

    @Test
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

    /**
     * disconnected conjunction with specific concepts
     */
    @Test
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

        Atom resolvableIsa = query.getAtoms(Atom.class).filter(at -> at.getVarNames().containsAll(Sets.newHashSet(var("x"), var("type")))).findFirst().orElse(null);
        assertThat(resolutionPlan.plan().get(3), is(resolvableIsa));

        checkQueryPlanComplete(query, new ResolutionQueryPlan(query));
    }

    @Test
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
        assertEquals(atomPlan, desiredAtomPlan);
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
            assertTrue(!Sets.intersection(varNames, vars).isEmpty());
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
            assertTrue(!Sets.intersection(varNames, vars).isEmpty());
            vars.addAll(varNames);
        }
    }

    private void checkAtomPlanComplete(ReasonerQueryImpl query, ResolutionPlan plan){
        assertEquals(query.selectAtoms(), Sets.newHashSet(plan.plan()) );
    }

    private void checkQueryPlanComplete(ReasonerQueryImpl query, ResolutionQueryPlan plan){
        assertEquals(query.selectAtoms(), plan.queries().stream().flatMap(q -> q.selectAtoms().stream()).collect(toSet()));
    }

    private Atom getAtom(ReasonerQueryImpl query, String typeString, GraknTx tx){
        Type type = tx.getType(Label.of(typeString));
        return query.getAtoms(Atom.class).filter(at -> at.getTypeId().equals(type.getId())).findFirst().orElse(null);
    }

    private Conjunction<VarPatternAdmin> conjunction(String patternString, GraknTx graph){
        Set<VarPatternAdmin> vars = graph.graql().parser().parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }
}
