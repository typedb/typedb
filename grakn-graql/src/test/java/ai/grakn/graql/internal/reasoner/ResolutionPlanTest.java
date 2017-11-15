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
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.test.rule.SampleKBContext;
import com.google.common.collect.ImmutableList;
import java.util.Set;
import org.junit.ClassRule;
import org.junit.Test;

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;

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
