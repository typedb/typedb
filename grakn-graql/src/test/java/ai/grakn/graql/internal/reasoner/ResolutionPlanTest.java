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
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.plan.ResolutionPlan;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.test.rule.SampleKBContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.collect.UnmodifiableIterator;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static ai.grakn.graql.Graql.var;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class ResolutionPlanTest {

    @ClassRule
    public static final SampleKBContext testContext = SampleKBContext.load("resolution-plan-test.gql");

    @Test
    public void makeSureDisconnectedIndexedQueriesProduceCompletePlan_indexedResource() {
        EmbeddedGraknTx<?> testTx = testContext.tx();
        String queryString = "{" +
                "$x isa someEntity;" +
                "$y isa resource; $y 'value';" +
                "$z isa relation;" +
                "}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(queryString, testTx), testTx);
        new ResolutionPlan(query);
    }

    @Test
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
        ImmutableList<Atom> plan = new ResolutionPlan(query).plan();
        assertEquals(plan, correctPlan);
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
        ImmutableList<Atom> plan = new ResolutionPlan(query).plan();
        assertEquals(plan, correctPlan);
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
        ImmutableList<Atom> plan = new ResolutionPlan(query).plan();
        assertEquals(plan, correctPlan);
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
        ImmutableList<Atom> plan = new ResolutionPlan(query).plan();
        assertEquals(plan, correctPlan);
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
        ImmutableList<Atom> plan = new ResolutionPlan(query).plan();

        UnmodifiableIterator<Atom> iterator = plan.iterator();
        Set<Var> vars = new HashSet<>();
        vars.addAll(iterator.next().getVarNames());
        while(iterator.hasNext()){
            Atom next = iterator.next();
            Set<Var> varNames = next.getVarNames();
            assertTrue(!Sets.intersection(varNames, vars).isEmpty());
            vars.addAll(varNames);
        }
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
        ImmutableList<Atom> plan = new ResolutionPlan(query).plan();

        UnmodifiableIterator<Atom> iterator = plan.iterator();
        Set<Var> vars = new HashSet<>();
        vars.addAll(iterator.next().getVarNames());
        while(iterator.hasNext()){
            Atom next = iterator.next();
            Set<Var> varNames = next.getVarNames();
            assertTrue(!Sets.intersection(varNames, vars).isEmpty());
            vars.addAll(varNames);
        }
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
        ImmutableList<Atom> plan = new ResolutionPlan(query).plan();
        assertTrue(plan.containsAll(query.selectAtoms()));
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
                "$x id '" + concept.id() + "';" +
                basePatternString +
                "}";
        String yPatternString = "{" +
                "$y id '" + concept.id() + "';" +
                basePatternString +
                "}";
        ReasonerQueryImpl queryX = ReasonerQueries.create(conjunction(xPatternString, testTx), testTx);
        ReasonerQueryImpl queryY = ReasonerQueries.create(conjunction(yPatternString, testTx), testTx);
        assertNotEquals(new ResolutionPlan(queryX).plan().get(0), getAtom(queryX, "anotherResource", testTx));
        assertNotEquals(new ResolutionPlan(queryY).plan().get(0), getAtom(queryX, "resource", testTx));
    }

    private Atom getAtom(ReasonerQueryImpl query, String typeString, GraknTx tx){
        Type type = tx.getType(Label.of(typeString));
        return query.getAtoms(Atom.class).filter(at -> at.getTypeId().equals(type.id())).findFirst().orElse(null);
    }

    private Conjunction<VarPatternAdmin> conjunction(String patternString, GraknTx graph){
        Set<VarPatternAdmin> vars = graph.graql().parser().parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }
}
