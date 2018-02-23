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

package ai.grakn.graql.internal.query;

import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;
import ai.grakn.graql.internal.gremlin.GreedyTraversalPlan;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;
import ai.grakn.graql.internal.gremlin.fragment.NeqFragment;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.test.rule.SampleKBContext;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static ai.grakn.graql.Graql.and;
import static ai.grakn.graql.Graql.var;
import static org.junit.Assert.assertEquals;

public class QueryPlannerTest {

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
    public void shardCountIsUsed() {
        // force the concept to get a new shard
        // shards of thing = 2 (thing = 1 and thing itself)
        // thing 2 = 4, thing3 = 7
        tx.shard(tx.getEntityType(thingy2).getId());
        tx.shard(tx.getEntityType(thingy2).getId());
        tx.shard(tx.getEntityType(thingy2).getId());

        tx.shard(tx.getEntityType(thingy3).getId());
        tx.shard(tx.getEntityType(thingy3).getId());
        tx.shard(tx.getEntityType(thingy3).getId());
        tx.shard(tx.getEntityType(thingy3).getId());
        tx.shard(tx.getEntityType(thingy3).getId());
        tx.shard(tx.getEntityType(thingy3).getId());

        Pattern pattern;
        ImmutableList<Fragment> plan;

        pattern = and(
                x.isa(thingy1),
                y.isa(thingy2),
                z.isa(thingy3),
                var().rel(x).rel(y).rel(z));
        plan = getPlan(pattern);
        assertEquals(x, plan.get(3).end());
        assertEquals(3L, plan.stream().filter(fragment -> fragment instanceof NeqFragment).count());

        //TODO: should uncomment the following after updating cost of out-isa fragment
//        varName = plan.get(7).end().getValue();
//        assertEquals(y.getValue(), varName);
//
//        varName = plan.get(11).end().getValue();
//        assertEquals(y.getValue(), varName);

        pattern = and(
                x.isa(thingy),
                y.isa(thingy2),
                var().rel(x).rel(y));
        plan = getPlan(pattern);
        assertEquals(x, plan.get(3).end());

        pattern = and(
                x.isa(thingy),
                y.isa(thingy2),
                z.isa(thingy3),
                var().rel(x).rel(y).rel(z));
        plan = getPlan(pattern);
        assertEquals(x, plan.get(4).end());

        pattern = and(
                x.isa(superType),
                superType.label(thingy),
                y.isa(thingy2),
                subType.sub(superType),
                z.isa(subType),
                var().rel(x).rel(y));
        plan = getPlan(pattern);
        assertEquals(z, plan.get(4).end());

        tx.shard(tx.getEntityType(thingy1).getId());
        tx.shard(tx.getEntityType(thingy1).getId());
        tx.shard(tx.getEntityType(thingy).getId());
        // now thing = 5, thing1 = 3

        pattern = and(
                x.isa(thingy),
                y.isa(thingy2),
                z.isa(thingy3),
                var().rel(x).rel(y).rel(z));
        plan = getPlan(pattern);
        assertEquals(y, plan.get(3).end());

        pattern = and(
                x.isa(thingy1),
                y.isa(thingy2),
                z.isa(thingy3),
                var().rel(x).rel(y).rel(z));
        plan = getPlan(pattern);
        assertEquals(x, plan.get(3).end());

        tx.shard(tx.getEntityType(thingy1).getId());
        tx.shard(tx.getEntityType(thingy1).getId());
        // now thing = 7, thing1 = 5

        pattern = and(
                x.isa(thingy),
                y.isa(thingy2),
                z.isa(thingy3),
                var().rel(x).rel(y).rel(z));
        plan = getPlan(pattern);
        assertEquals(y, plan.get(3).end());

        pattern = and(
                x.isa(thingy1),
                y.isa(thingy2),
                z.isa(thingy3),
                var().rel(x).rel(y).rel(z));
        plan = getPlan(pattern);
        assertEquals(y, plan.get(3).end());
    }

    private ImmutableList<Fragment> getPlan(Pattern pattern) {
        return GreedyTraversalPlan.createTraversal(pattern.admin(), tx).fragments().iterator().next();
    }
}