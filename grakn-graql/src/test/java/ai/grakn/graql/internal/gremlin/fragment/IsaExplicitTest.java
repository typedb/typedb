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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.graql.AggregateQuery;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Match;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import ai.grakn.graql.answer.Value;
import ai.grakn.graql.internal.gremlin.GreedyTraversalPlan;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.test.rule.SampleKBContext;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static ai.grakn.graql.Graql.and;
import static ai.grakn.graql.Graql.var;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class IsaExplicitTest {

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

        EntityType superType1 = graph.putEntityType(thingy);
        entityType0.sup(superType1);
        entityType1.sup(superType1);

        Role role1 = graph.putRole("role1");
        Role role2 = graph.putRole("role2");
        Role role3 = graph.putRole("role3");
        superType1.plays(role1).plays(role2).plays(role3);
        entityType2.plays(role1).plays(role2).plays(role3);
        entityType3.plays(role1).plays(role2).plays(role3);
        RelationshipType relationshipType = graph.putRelationshipType(related)
                .relates(role1).relates(role2).relates(role3);

        Entity entity1 = entityType1.create();
        Entity entity2 = entityType2.create();
        Entity entity3 = entityType3.create();
        relationshipType.create()
                .assign(role1, entity1)
                .assign(role2, entity2)
                .assign(role3, entity3);
    });

    @Before
    public void setUp() {
        tx = context.tx();
    }

    @Test
    public void testInsertSyntax() {
        QueryBuilder queryBuilder = tx.graql();
        InsertQuery insertQuery;

        insertQuery = queryBuilder.insert(x.isaExplicit(thingy));
        assertEquals("insert $x isa! thingy;", insertQuery.toString());

        insertQuery = queryBuilder.parse("insert $x isa! thingy;");
        assertEquals(queryBuilder.insert(x.isaExplicit(thingy)), insertQuery);
    }

    @Test
    public void whenInsertIsaExplicit_InsertsADirectInstanceOfAType() {
        QueryBuilder queryBuilder = tx.graql();
        queryBuilder.insert(x.isaExplicit(thingy)).execute();
        assertEquals(1, queryBuilder.<AggregateQuery<Value>>parse("match $z isa! thingy; aggregate count;").execute().get(0).number().intValue());
        assertEquals(2, queryBuilder.<AggregateQuery<Value>>parse("match $z isa thingy; aggregate count;").execute().get(0).number().intValue());
    }

    @Test
    public void testMatchSyntax() {
        QueryBuilder queryBuilder = tx.graql();
        Match matchQuery;
        GetQuery getQuery;

        matchQuery = queryBuilder.match(x.isaExplicit(thingy1));
        assertEquals("match $x isa! thingy1;", matchQuery.toString());

        matchQuery = queryBuilder.match(x.isaExplicit(y));
        assertEquals("match $x isa! $y;", matchQuery.toString());

        getQuery = queryBuilder.parse("match $x isa! thingy1; get;");
        assertEquals(queryBuilder.match(x.isaExplicit(thingy1)), getQuery.match());

        getQuery = queryBuilder.parse("match $x isa! $y; get;");
        assertEquals(queryBuilder.match(x.isaExplicit(y)), getQuery.match());
    }

    @Test
    public void testMatchIsaAndIsaExplicitReturnDifferentPlans() {
        Pattern pattern;
        ImmutableList<Fragment> plan;

        // test type without subtypes

        pattern = x.isaExplicit(thingy1);
        plan = getPlan(pattern);
        assertEquals(2, plan.size());
        assertThat(plan, contains(
                instanceOf(LabelFragment.class),
                instanceOf(InIsaFragment.class)
        ));

        pattern = and(
                x.isaExplicit(thingy1),
                y.isaExplicit(thingy1),
                var().rel(x).rel(y).isa(related));
        plan = getPlan(pattern);

        assertEquals(9, plan.size());
        // 3 labels: thingy1, thingy1 and related
        assertThat(plan, contains(
                instanceOf(LabelFragment.class),
                instanceOf(LabelFragment.class),
                instanceOf(LabelFragment.class),
                instanceOf(InIsaFragment.class), // start from relationship type
                instanceOf(OutRolePlayerFragment.class), // go to a role player
                instanceOf(OutIsaFragment.class), // check the role player's type
                instanceOf(OutRolePlayerFragment.class), // go to the other role player
                instanceOf(NeqFragment.class), // check two players are different
                instanceOf(OutIsaFragment.class) // check the role player's type
        ));

        // test type with subtypes

        pattern = x.isaExplicit(thingy);
        plan = getPlan(pattern);
        assertEquals(2, plan.size());
        assertThat(plan, contains(
                instanceOf(LabelFragment.class),
                instanceOf(InIsaFragment.class)
        ));

        pattern = x.isa(thingy);
        plan = getPlan(pattern);

        assertEquals(3, plan.size());
        assertThat(plan, contains(
                instanceOf(LabelFragment.class),
                instanceOf(InSubFragment.class),
                instanceOf(InIsaFragment.class)
        ));

        pattern = and(
                x.isaExplicit(thingy),
                y.isaExplicit(thingy),
                var().rel(x).rel(y).isa(related));
        plan = getPlan(pattern);

        assertEquals(9, plan.size());
        // 3 labels: thingy1, thingy1 and related
        assertThat(plan, contains(
                instanceOf(LabelFragment.class),
                instanceOf(LabelFragment.class),
                instanceOf(LabelFragment.class),
                instanceOf(InIsaFragment.class), // start from relationship type
                instanceOf(OutRolePlayerFragment.class), // go to a role player
                instanceOf(OutIsaFragment.class), // check the role player's type
                instanceOf(OutRolePlayerFragment.class), // go to the other role player
                instanceOf(NeqFragment.class), // check two players are different
                instanceOf(OutIsaFragment.class) // check the role player's type
        ));

        // combine isa and explicit isa

        pattern = and(
                x.isa(thingy),
                y.isaExplicit(thingy),
                var().rel(x).rel(y).isa(related));
        plan = getPlan(pattern);

        assertEquals(10, plan.size());
        // 3 labels: thingy, thingy and related
        assertThat(plan, contains(
                instanceOf(LabelFragment.class),
                instanceOf(LabelFragment.class),
                instanceOf(LabelFragment.class),
                instanceOf(InIsaFragment.class), // start from relationship type
                instanceOf(OutRolePlayerFragment.class), // go to a role player
                instanceOf(OutIsaFragment.class), // check the role player's type
                instanceOf(OutRolePlayerFragment.class), // go to the other role player
                instanceOf(NeqFragment.class), // check two players are different
                instanceOf(OutIsaFragment.class), // check the role player's type
                instanceOf(OutSubFragment.class) // check the subtypes
        ));
    }

    private ImmutableList<Fragment> getPlan(Pattern pattern) {
        return GreedyTraversalPlan.createTraversal(pattern.admin(), tx).fragments().iterator().next();
    }
}