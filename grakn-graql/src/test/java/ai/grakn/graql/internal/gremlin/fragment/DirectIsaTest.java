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

package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Match;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import ai.grakn.graql.internal.gremlin.GreedyTraversalPlan;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.test.rule.SampleKBContext;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static ai.grakn.graql.Graql.and;
import static ai.grakn.graql.Graql.var;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DirectIsaTest {

    private static final Var x = var("x");
    private static final Var y = var("y");
    private static final Var z = var("z");

    private static final Var superType = var("superType");
    private static final Var subType = var("subType");

    private static final String thingy = "thingy";
    private static final String thingy0 = "thingy0";
    private static final String thingy1 = "thingy1";
    private static final String thingy2 = "thingy2";
    private static final String thingy3 = "thingy3";
    private static final String related = "related";

    private EmbeddedGraknTx<?> tx;

    @ClassRule
    public static final SampleKBContext context = SampleKBContext.load(graph -> {

        EntityType entityType0 = graph.putEntityType(thingy0);
        EntityType entityType1 = graph.putEntityType(thingy1);
        EntityType entityType2 = graph.putEntityType(thingy2);
        EntityType entityType3 = graph.putEntityType(thingy3);

        EntityType superType1 = graph.putEntityType(thingy)
                .sub(entityType0)
                .sub(entityType1);

        Role role1 = graph.putRole("role1");
        Role role2 = graph.putRole("role2");
        Role role3 = graph.putRole("role3");
        superType1.plays(role1).plays(role2).plays(role3);
        entityType2.plays(role1).plays(role2).plays(role3);
        entityType3.plays(role1).plays(role2).plays(role3);
        RelationshipType relationshipType = graph.putRelationshipType(related)
                .relates(role1).relates(role2).relates(role3);

        Entity entity1 = entityType1.addEntity();
        Entity entity2 = entityType2.addEntity();
        Entity entity3 = entityType3.addEntity();
        relationshipType.addRelationship()
                .addRolePlayer(role1, entity1)
                .addRolePlayer(role2, entity2)
                .addRolePlayer(role3, entity3);
    });

    @Before
    public void setUp() {
        tx = context.tx();
    }

    @Test
    public void testInsertSyntax() {
        QueryBuilder queryBuilder = tx.graql();
        InsertQuery insertQuery;

        insertQuery = queryBuilder.insert(x.directIsa(thingy));
        assertEquals("insert $x isa! thingy;", insertQuery.toString());

        insertQuery = queryBuilder.parse("insert $x isa! thingy;");
        assertEquals(queryBuilder.insert(x.directIsa(thingy)), insertQuery);
    }

    @Test
    public void whenInsertDirectIsa_InsertsADirectInstanceOfAType() {
        QueryBuilder queryBuilder = tx.graql();
        queryBuilder.insert(x.directIsa(thingy)).execute();
        assertEquals(1L, queryBuilder.parse("match $z isa! thingy; aggregate count;").execute());
        assertEquals(2L, queryBuilder.parse("match $z isa thingy; aggregate count;").execute());
    }

    @Test
    public void testMatchSyntax() {
        QueryBuilder queryBuilder = tx.graql();
        Match matchQuery;
        GetQuery getQuery;

        matchQuery = queryBuilder.match(x.directIsa(thingy1));
        assertEquals("match $x isa! thingy1;", matchQuery.toString());

        matchQuery = queryBuilder.match(x.directIsa(y));
        assertEquals("match $x isa! $y;", matchQuery.toString());

        getQuery = queryBuilder.parse("match $x isa! thingy1; get;");
        assertEquals(queryBuilder.match(x.directIsa(thingy1)), getQuery.match());

        getQuery = queryBuilder.parse("match $x isa! $y; get;");
        assertEquals(queryBuilder.match(x.directIsa(y)), getQuery.match());
    }

    @Test
    public void testMatchIsaAndDirectIsaReturnDifferentPlans() {
        Pattern pattern;
        ImmutableList<Fragment> plan;

        // test type without subtypes

        pattern = x.directIsa(thingy1);
        plan = getPlan(pattern);
        assertEquals(2, plan.size());
        assertTrue(plan.get(0) instanceof LabelFragment);
        assertTrue(plan.get(1) instanceof InIsaFragment);

        pattern = and(
                x.directIsa(thingy1),
                y.directIsa(thingy1),
                var().rel(x).rel(y).isa(related));
        plan = getPlan(pattern);

        assertEquals(9, plan.size());
        // 3 labels: thingy1, thingy1 and related
        assertTrue(plan.get(0) instanceof LabelFragment);
        assertTrue(plan.get(1) instanceof LabelFragment);
        assertTrue(plan.get(2) instanceof LabelFragment);
        assertTrue(plan.get(3) instanceof InIsaFragment); // start from relationship type
        assertTrue(plan.get(4) instanceof OutRolePlayerFragment); // go to a role player
        assertTrue(plan.get(5) instanceof OutIsaFragment); // check the role player's type
        assertTrue(plan.get(6) instanceof OutRolePlayerFragment); // go to the other role player
        assertTrue(plan.get(7) instanceof NeqFragment); // check two players are different
        assertTrue(plan.get(8) instanceof OutIsaFragment); // check the role player's type

        // test type with subtypes

        pattern = x.directIsa(thingy);
        plan = getPlan(pattern);
        assertEquals(2, plan.size());
        assertTrue(plan.get(0) instanceof LabelFragment);
        assertTrue(plan.get(1) instanceof InIsaFragment);

        pattern = x.isa(thingy);
        plan = getPlan(pattern);

        assertEquals(3, plan.size());
        assertTrue(plan.get(0) instanceof LabelFragment);
        assertTrue(plan.get(1) instanceof InSubFragment);
        assertTrue(plan.get(2) instanceof InIsaFragment);

        pattern = and(
                x.directIsa(thingy),
                y.directIsa(thingy),
                var().rel(x).rel(y).isa(related));
        plan = getPlan(pattern);

        assertEquals(9, plan.size());
        // 3 labels: thingy1, thingy1 and related
        assertTrue(plan.get(0) instanceof LabelFragment);
        assertTrue(plan.get(1) instanceof LabelFragment);
        assertTrue(plan.get(2) instanceof LabelFragment);
        assertTrue(plan.get(3) instanceof InIsaFragment); // start from relationship type
        assertTrue(plan.get(4) instanceof OutRolePlayerFragment); // go to a role player
        assertTrue(plan.get(5) instanceof OutIsaFragment); // check the role player's type
        assertTrue(plan.get(6) instanceof OutRolePlayerFragment); // go to the other role player
        assertTrue(plan.get(7) instanceof NeqFragment); // check two players are different
        assertTrue(plan.get(8) instanceof OutIsaFragment); // check the role player's type

        // combine isa and direct isa

        pattern = and(
                x.isa(thingy),
                y.directIsa(thingy),
                var().rel(x).rel(y).isa(related));
        plan = getPlan(pattern);

        assertEquals(10, plan.size());
        // 3 labels: thingy, thingy and related
        assertTrue(plan.get(0) instanceof LabelFragment);
        assertTrue(plan.get(1) instanceof LabelFragment);
        assertTrue(plan.get(2) instanceof LabelFragment);
        assertTrue(plan.get(3) instanceof InIsaFragment); // start from relationship type
        assertTrue(plan.get(4) instanceof OutRolePlayerFragment); // go to a role player with direct isa
        assertTrue(plan.get(5) instanceof OutIsaFragment); // check the role player's type
        assertTrue(plan.get(6) instanceof OutRolePlayerFragment); // go to the other role player
        assertTrue(plan.get(7) instanceof NeqFragment); // check two players are different
        assertTrue(plan.get(8) instanceof OutIsaFragment); // check the role player's type
        assertTrue(plan.get(9) instanceof OutSubFragment); // check the subtypes
    }

    private ImmutableList<Fragment> getPlan(Pattern pattern) {
        return GreedyTraversalPlan.createTraversal(pattern.admin(), tx).fragments().iterator().next();
    }
}