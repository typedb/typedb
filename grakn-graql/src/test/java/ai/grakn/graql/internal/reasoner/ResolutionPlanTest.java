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

package ai.grakn.graql.internal.reasoner;

import ai.grakn.GraknTx;
import ai.grakn.concept.Label;
import ai.grakn.concept.Type;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.test.rule.SampleKBContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.collect.UnmodifiableIterator;
import java.util.HashSet;
import java.util.Set;
import org.junit.ClassRule;
import org.junit.Test;

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ResolutionPlanTest {

    @ClassRule
    public static final SampleKBContext testContext = SampleKBContext.load("resolution-plan-test.gql");


    @Test
    public void prioritiseSubbedRelationsOverNonSubbedOnes() {
        GraknTx testTx = testContext.tx();
        String queryString = "{" +
                "(role1:$x, role2: $y) isa relation;" +
                "(role1:$y, role2: $z) isa anotherRelation;" +
                "(role1:$z, role2: $w) isa yetAnotherRelation;" +
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
        GraknTx testTx = testContext.tx();
        String queryString = "{" +
                "(role1:$x, role2: $y) isa relation;" +
                "(role1:$y, role2: $z) isa anotherRelation;" +
                "(role1:$z, role2: $w) isa yetAnotherRelation;" +
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
        GraknTx testTx = testContext.tx();
        String queryString = "{" +
                "(role1:$x, role2: $y) isa relation;" +
                "(role1:$y, role2: $z) isa anotherRelation;" +
                "(role1:$z, role2: $w) isa yetAnotherRelation;" +
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
        GraknTx testTx = testContext.tx();
        String queryString = "{" +
                "(role1:$x, role2: $y) isa relation;" +
                "(role1:$y, role2: $z) isa anotherRelation;" +
                "(role1:$z, role2: $w) isa yetAnotherRelation;" +
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
        GraknTx testTx = testContext.tx();
        String queryString = "{" +
                "(role1:$x, role2: $y) isa relation;" +
                "(role1:$y, role2: $z) isa anotherRelation;" +
                "(role1:$z, role2: $w) isa relation;" +
                "(role1:$w, role2: $u) isa anotherRelation;" +
                "(role1:$u, role2: $v) isa relation;" +
                "}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(queryString, testTx), testTx);
        ImmutableList<Atom> plan = new ResolutionPlan(query).plan();
        checkConnectedness(plan);
    }

    @Test
    public void makeSureConnectednessPreservedWhenRelationsWithSameTypesPresent_someRelationsRuleResolvable(){
        GraknTx testTx = testContext.tx();
        String queryString = "{" +
                "(role1:$x, role2: $y) isa relation;" +
                "(role1:$y, role2: $z) isa recursiveRelation;" +
                "(role1:$z, role2: $w) isa relation;" +
                "(role1:$w, role2: $u) isa recursiveRelation;" +
                "(role1:$u, role2: $v) isa relation;" +
                "}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(queryString, testTx), testTx);
        ImmutableList<Atom> plan = new ResolutionPlan(query).plan();
        checkConnectedness(plan);
    }

    @Test
    public void makeSureConnectednessPreservedWhenRelationsWithSameTypesPresent_longerChain(){
        GraknTx testTx = testContext.tx();
        String queryString = "{" +
                "(role1:$x, role2: $y) isa relation;" +
                "(role1:$y, role2: $z) isa anotherRelation;" +
                "(role1:$z, role2: $w) isa yetAnotherRelation;" +
                "(role1:$w, role2: $u) isa relation;" +
                "(role1:$u, role2: $v) isa anotherRelation;" +
                "(role1:$v, role2: $q) isa yetAnotherRelation;"+
                "}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(queryString, testTx), testTx);
        ImmutableList<Atom> plan = new ResolutionPlan(query).plan();
        checkConnectedness(plan);
    }

    @Test
    public void makeSureConnectednessPreservedWhenRelationsWithSameTypesPresent_longerChain_someRelationsRuleResolvable(){
        GraknTx testTx = testContext.tx();
        String queryString = "{" +
                "(role1:$x, role2: $y) isa relation;" +
                "(role1:$y, role2: $z) isa anotherRelation;" +
                "(role1:$z, role2: $w) isa recursiveRelation;" +
                "(role1:$w, role2: $u) isa relation;" +
                "(role1:$u, role2: $v) isa anotherRelation;" +
                "(role1:$v, role2: $q) isa recursiveRelation;"+
                "}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(queryString, testTx), testTx);
        ImmutableList<Atom> plan = new ResolutionPlan(query).plan();
        checkConnectedness(plan);
    }

    @Test
    public void makeSureConnectednessPreservedWhenRelationsWithSameTypesPresent_circularPattern_someRelationsRuleResolvable(){
        GraknTx testTx = testContext.tx();
        String queryString = "{" +
                "(role1:$x, role2: $y) isa relation;" +
                "(role1:$x, role2: $w) isa anotherRelation;" +
                "(role1:$y, role2: $z) isa recursiveRelation;" +
                "(role1:$z, role2: $w) isa relation;" +
                "}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(queryString, testTx), testTx);
        ImmutableList<Atom> plan = new ResolutionPlan(query).plan();
        checkConnectedness(plan);
    }

    @Test
    public void makeSureNonResolvableRelationFavouredInResolvable_NonResolvablePair(){
        GraknTx testTx = testContext.tx();
        String queryString = "{" +
                "(role1:$x, role2: $y) isa relation;" +
                "(role1:$y, role2: $z) isa recursiveRelation;" +
                "}";

        String queryString2 = "{" +
                "(role1:$x, role2: $y) isa recursiveRelation;" +
                "(role1:$y, role2: $z) isa relation;" +
                "}";
        ReasonerQueryImpl query = ReasonerQueries.create(conjunction(queryString, testTx), testTx);
        ReasonerQueryImpl query2 = ReasonerQueries.create(conjunction(queryString2, testTx), testTx);
        ImmutableList<Atom> correctPlan = ImmutableList.of(
                getAtom(query, "relation", testTx),
                getAtom(query, "recursiveRelation", testTx)
        );
        ImmutableList<Atom> correctPlan2 = ImmutableList.of(
                getAtom(query2, "relation", testTx),
                getAtom(query2, "recursiveRelation", testTx)
        );
        ImmutableList<Atom> plan = new ResolutionPlan(query).plan();
        ImmutableList<Atom> plan2 = new ResolutionPlan(query2).plan();
        assertEquals(plan, correctPlan);
        assertEquals(plan2, correctPlan2);
    }

    private void checkConnectedness(ImmutableList<Atom> plan){
        UnmodifiableIterator<Atom> iterator = plan.iterator();
        Set<Var> vars = new HashSet<>();
        iterator.next().getVarNames().forEach(vars::add);
        while(iterator.hasNext()){
            Atom next = iterator.next();
            Set<Var> varNames = next.getVarNames();
            assertTrue(!Sets.intersection(varNames, vars).isEmpty());
            varNames.forEach(vars::add);
        }
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
