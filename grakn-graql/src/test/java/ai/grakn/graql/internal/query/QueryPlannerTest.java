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

package ai.grakn.graql.internal.query;

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;
import ai.grakn.graql.internal.gremlin.GreedyTraversalPlan;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;
import ai.grakn.graql.internal.gremlin.fragment.InIsaFragment;
import ai.grakn.graql.internal.gremlin.fragment.LabelFragment;
import ai.grakn.graql.internal.gremlin.fragment.NeqFragment;
import ai.grakn.graql.internal.gremlin.fragment.OutIsaFragment;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.test.rule.SampleKBContext;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static ai.grakn.graql.Graql.and;
import static ai.grakn.graql.Graql.var;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

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
    private static final String thingy4 = "thingy4";
    private static final String related = "related";
    private static final String veryRelated = "veryRelated";
    private static final String sameAsRelated = "sameAsRelated";

    private static final String resourceType = "resourceType";

    private EmbeddedGraknTx<?> tx;

    @ClassRule
    public static final SampleKBContext context = SampleKBContext.load(graph -> {

        EntityType entityType0 = graph.putEntityType(thingy0);
        EntityType entityType1 = graph.putEntityType(thingy1);
        EntityType entityType2 = graph.putEntityType(thingy2);
        EntityType entityType3 = graph.putEntityType(thingy3);
        EntityType entityType4 = graph.putEntityType(thingy4);

        AttributeType<String> attributeType = graph.putAttributeType(resourceType, AttributeType.DataType.STRING);
        entityType4.attribute(attributeType);

        EntityType superType1 = graph.putEntityType(thingy)
                .sub(entityType0)
                .sub(entityType1);

        Role role1 = graph.putRole("role1");
        Role role2 = graph.putRole("role2");
        Role role3 = graph.putRole("role3");
        superType1.plays(role1).plays(role2).plays(role3);
        entityType2.plays(role1).plays(role2).plays(role3);
        entityType3.plays(role1).plays(role2).plays(role3);
        RelationshipType relationshipType1 = graph.putRelationshipType(related)
                .relates(role1).relates(role2).relates(role3);
        graph.putRelationshipType(sameAsRelated)
                .relates(role1).relates(role2).relates(role3);

        Role role4 = graph.putRole("role4");
        Role role5 = graph.putRole("role5");
        entityType1.plays(role4).plays(role5);
        entityType2.plays(role4).plays(role5);
        entityType4.plays(role4);
        graph.putRelationshipType(veryRelated)
                .relates(role4).relates(role5);

        Entity entity1 = entityType1.addEntity();
        Entity entity2 = entityType2.addEntity();
        Entity entity3 = entityType3.addEntity();
        relationshipType1.addRelationship()
                .addRolePlayer(role1, entity1)
                .addRolePlayer(role2, entity2)
                .addRolePlayer(role3, entity3);
    });

    @Before
    public void setUp() {
        tx = context.tx();
    }

    @Test
    public void inferUniqueRelationshipType() {
        Pattern pattern;
        ImmutableList<Fragment> plan;

        pattern = and(
                x.isa(thingy1),
                y.isa(thingy2),
                var().rel(x).rel(y));
        plan = getPlan(pattern);
        assertEquals(2L, plan.stream().filter(LabelFragment.class::isInstance).count());

        pattern = and(
                x.isa(thingy),
                y.isa(thingy2),
                var().rel(x).rel(y));
        plan = getPlan(pattern);
        assertEquals(2L, plan.stream().filter(LabelFragment.class::isInstance).count());

        pattern = and(
                x.isa(thingy2),
                y.isa(thingy4),
                var().rel(x).rel(y));
        plan = getPlan(pattern);

        // Relationship type can now be inferred, so one more relationship type label
        assertEquals(3L, plan.stream().filter(LabelFragment.class::isInstance).count());

        // Should start from the inferred relationship type, instead of role players
        String relationship = plan.get(3).start().getValue();
        assertNotEquals(relationship, x.getValue());
        assertNotEquals(relationship, y.getValue());
    }

    @Test
    public void inferRelationshipTypeWithMoreThan2Roles() {
        Pattern pattern;
        ImmutableList<Fragment> plan;

        pattern = and(
                x.isa(thingy1),
                y.isa(thingy2),
                z.isa(thingy3),
                var().rel(x).rel(y).rel(z));
        plan = getPlan(pattern);
        assertEquals(3L, plan.stream().filter(LabelFragment.class::isInstance).count());

        pattern = and(
                x.isa(thingy1),
                y.isa(thingy2),
                z.isa(thingy4),
                var().rel(x).rel(y).rel(z));
        plan = getPlan(pattern);
        // Relationship type can now be inferred, so one more relationship type label
        assertEquals(4L, plan.stream().filter(LabelFragment.class::isInstance).count());
        assertEquals(4L, plan.stream()
                .filter(fragment -> fragment instanceof OutIsaFragment || fragment instanceof InIsaFragment).count());
    }

    @Test
    public void inferRelationshipTypeWithARolePlayerWithNoType() {
        Pattern pattern;
        ImmutableList<Fragment> plan;

        pattern = and(
                x.isa(thingy1),
                y.isa(thingy2),
                var().rel(x).rel(y).rel(z));
        plan = getPlan(pattern);
        assertEquals(2L, plan.stream().filter(LabelFragment.class::isInstance).count());

        pattern = and(
                y.isa(thingy2),
                z.isa(thingy4),
                var().rel(x).rel(y).rel(z));
        plan = getPlan(pattern);
        // Relationship type can now be inferred, so one more relationship type label
        assertEquals(3L, plan.stream().filter(LabelFragment.class::isInstance).count());
        assertEquals(3L, plan.stream()
                .filter(fragment -> fragment instanceof OutIsaFragment || fragment instanceof InIsaFragment).count());
    }

    @Test
    public void inferRelationshipTypeWhereRolePlayedBySuperType() {
        Pattern pattern;
        ImmutableList<Fragment> plan;

        pattern = and(
                x.isa(thingy),
                y.isa(thingy4),
                var().rel(x).rel(y));
        plan = getPlan(pattern);
        assertEquals(2L, plan.stream().filter(LabelFragment.class::isInstance).count());

        pattern = and(
                x.isa(thingy),
                y.isa(thingy2),
                z.isa(thingy4),
                var().rel(x).rel(y).rel(z));
        plan = getPlan(pattern);
        // Relationship type can now be inferred, so one more relationship type label
        assertEquals(4L, plan.stream().filter(LabelFragment.class::isInstance).count());
        assertEquals(4L, plan.stream()
                .filter(fragment -> fragment instanceof OutIsaFragment || fragment instanceof InIsaFragment).count());
    }

    @Test
    public void inferRelationshipTypeWhereAVarHasTwoTypes() {
        Pattern pattern;
        ImmutableList<Fragment> plan;

        pattern = and(
                x.isa(thingy),
                x.isa(thingy1),
                y.isa(thingy4),
                var().rel(x).rel(y));

        plan = getPlan(pattern);
        // Relationship type can now be inferred, so one more relationship type label plus existing 3 labels
        assertEquals(4L, plan.stream().filter(LabelFragment.class::isInstance).count());
        assertEquals(4L, plan.stream()
                .filter(fragment -> fragment instanceof OutIsaFragment || fragment instanceof InIsaFragment).count());

        // Should start from the inferred relationship type, instead of role players
        String relationship = plan.get(4).start().getValue();
        assertNotEquals(relationship, x.getValue());
        assertNotEquals(relationship, y.getValue());
    }

    @Test
    public void inferRelationshipTypeWhereAVarHasIncorrectTypes() {
        Pattern pattern;
        ImmutableList<Fragment> plan;

        pattern = and(
                x.isa(veryRelated),
                x.isa(thingy2),
                y.isa(thingy4),
                var().rel(x).rel(y));
        plan = getPlan(pattern);
        assertEquals(3L, plan.stream().filter(LabelFragment.class::isInstance).count());
        assertEquals(3L, plan.stream()
                .filter(fragment -> fragment instanceof OutIsaFragment || fragment instanceof InIsaFragment).count());

        pattern = and(
                x.isa(veryRelated),
                y.isa(thingy4),
                var().rel(x).rel(y));
        plan = getPlan(pattern);
        assertEquals(2L, plan.stream().filter(LabelFragment.class::isInstance).count());
        assertEquals(2L, plan.stream()
                .filter(fragment -> fragment instanceof OutIsaFragment || fragment instanceof InIsaFragment).count());
    }

    @Test
    public void avoidImplicitTypes() {
        Pattern pattern;
        ImmutableList<Fragment> plan;

        pattern = and(
                x.isa(thingy2),
                y.isa(thingy4),
                var().rel(x).rel(y));
        plan = getPlan(pattern);
        assertEquals(3L, plan.stream().filter(LabelFragment.class::isInstance).count());
        String relationship = plan.get(4).start().getValue();

        // should start from relationship
        assertNotEquals(relationship, x.getValue());
        assertNotEquals(relationship, y.getValue());

        pattern = and(
                x.isa(resourceType),
                y.isa(thingy4),
                var().rel(x).rel(y));
        plan = getPlan(pattern);
        assertEquals(3L, plan.stream().filter(LabelFragment.class::isInstance).count());
        relationship = plan.get(4).start().getValue();

        // should start from a role player
        assertTrue(relationship.equals(x.getValue()) || relationship.equals(y.getValue()));
        assertTrue(plan.get(5) instanceof OutIsaFragment);
    }

    @Test
    public void sameLabelFragmentShouldNotBeAddedTwice() {
        Pattern pattern;
        ImmutableList<Fragment> plan;

        pattern = and(
                x.isa(thingy2),
                y.isa(thingy4),
                z.isa(thingy2),
                var().rel(x).rel(y),
                var().rel(z).rel(y));
        plan = getPlan(pattern);

        // 2 thingy2 label, 1 thingy4, 1 inferred relationship label
        assertEquals(4L, plan.stream().filter(LabelFragment.class::isInstance).count());

        // 5 isa fragments: x, y, z, relationship between x and y, relationship between z and y
        assertEquals(5L, plan.stream()
                .filter(fragment -> fragment instanceof OutIsaFragment || fragment instanceof InIsaFragment).count());
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