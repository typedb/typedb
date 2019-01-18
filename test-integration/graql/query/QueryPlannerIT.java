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

package grakn.core.graql.query;

import com.google.common.collect.ImmutableList;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.Entity;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.RelationType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.internal.gremlin.GreedyTraversalPlan;
import grakn.core.graql.internal.gremlin.fragment.Fragment;
import grakn.core.graql.internal.gremlin.fragment.InIsaFragment;
import grakn.core.graql.internal.gremlin.fragment.LabelFragment;
import grakn.core.graql.internal.gremlin.fragment.NeqFragment;
import grakn.core.graql.internal.gremlin.fragment.OutIsaFragment;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.statement.Statement;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
import grakn.core.server.session.TransactionOLTP;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static grakn.core.graql.query.Graql.and;
import static grakn.core.graql.query.Graql.var;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class QueryPlannerIT {

    private static final Statement x = var("x");
    private static final Statement y = var("y");
    private static final Statement z = var("z");

    private static final Statement superType = var("superType");
    private static final Statement subType = var("subType");

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

    @ClassRule
    public static GraknTestServer graknServer = new GraknTestServer();
    private static Session session;
    private Transaction tx;

    @BeforeClass
    public static void newSession() {
        session = graknServer.sessionWithNewKeyspace();
        Transaction graph = session.transaction(Transaction.Type.WRITE);

        EntityType entityType0 = graph.putEntityType(thingy0);
        EntityType entityType1 = graph.putEntityType(thingy1);
        EntityType entityType2 = graph.putEntityType(thingy2);
        EntityType entityType3 = graph.putEntityType(thingy3);
        EntityType entityType4 = graph.putEntityType(thingy4);

        AttributeType<String> attributeType = graph.putAttributeType(resourceType, AttributeType.DataType.STRING);
        entityType4.has(attributeType);

        EntityType superType1 = graph.putEntityType(thingy);
        entityType0.sup(superType1);
        entityType1.sup(superType1);

        Role role1 = graph.putRole("role1");
        Role role2 = graph.putRole("role2");
        Role role3 = graph.putRole("role3");
        superType1.plays(role1).plays(role2).plays(role3);
        entityType2.plays(role1).plays(role2).plays(role3);
        entityType3.plays(role1).plays(role2).plays(role3);
        RelationType relationshipType1 = graph.putRelationshipType(related)
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

        Entity entity1 = entityType1.create();
        Entity entity2 = entityType2.create();
        Entity entity3 = entityType3.create();
        relationshipType1.create()
                .assign(role1, entity1)
                .assign(role2, entity2)
                .assign(role3, entity3);

        graph.commit();
    }

    @Before
    public void newTransaction() {
        tx = session.transaction(Transaction.Type.WRITE);
    }

    @After
    public void closeTransaction() {
        tx.close();
    }

    @AfterClass
    public static void closeSession() {
        session.close();
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
        String relationship = plan.get(3).start().name();
        assertNotEquals(relationship, x.var().name());
        assertNotEquals(relationship, y.var().name());
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
        String relationship = plan.get(4).start().name();
        assertNotEquals(relationship, x.var().name());
        assertNotEquals(relationship, y.var().name());
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
        String relationship = plan.get(4).start().name();

        // should start from relationship
        assertNotEquals(relationship, x.var().name());
        assertNotEquals(relationship, y.var().name());

        pattern = and(
                x.isa(resourceType),
                y.isa(thingy4),
                var().rel(x).rel(y));
        plan = getPlan(pattern);
        assertEquals(3L, plan.stream().filter(LabelFragment.class::isInstance).count());
        relationship = plan.get(4).start().name();

        // should start from a role player
        assertTrue(relationship.equals(x.var().name()) || relationship.equals(y.var().name()));
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

    @Test @SuppressWarnings("Duplicates")
    public void shardCountIsUsed() {
        // force the concept to get a new shard
        // shards of thing = 2 (thing = 1 and thing itself)
        // thing 2 = 4, thing3 = 7
        ((TransactionOLTP) tx).shard(tx.getEntityType(thingy2).id());
        ((TransactionOLTP) tx).shard(tx.getEntityType(thingy2).id());
        ((TransactionOLTP) tx).shard(tx.getEntityType(thingy2).id());

        ((TransactionOLTP) tx).shard(tx.getEntityType(thingy3).id());
        ((TransactionOLTP) tx).shard(tx.getEntityType(thingy3).id());
        ((TransactionOLTP) tx).shard(tx.getEntityType(thingy3).id());
        ((TransactionOLTP) tx).shard(tx.getEntityType(thingy3).id());
        ((TransactionOLTP) tx).shard(tx.getEntityType(thingy3).id());
        ((TransactionOLTP) tx).shard(tx.getEntityType(thingy3).id());

        Pattern pattern;
        ImmutableList<Fragment> plan;

        pattern = and(
                x.isa(thingy1),
                y.isa(thingy2),
                z.isa(thingy3),
                var().rel(x).rel(y).rel(z));
        plan = getPlan(pattern);
        assertEquals(x.var(), plan.get(3).end());
        assertEquals(3L, plan.stream().filter(fragment -> fragment instanceof NeqFragment).count());

        //TODO: should uncomment the following after updating cost of out-isa fragment
//        varName = plan.get(7).end().value();
//        assertEquals(y.value(), varName);
//
//        varName = plan.get(11).end().value();
//        assertEquals(y.value(), varName);

        pattern = and(
                x.isa(thingy),
                y.isa(thingy2),
                var().rel(x).rel(y));
        plan = getPlan(pattern);
        assertEquals(x.var(), plan.get(3).end());

        pattern = and(
                x.isa(thingy),
                y.isa(thingy2),
                z.isa(thingy3),
                var().rel(x).rel(y).rel(z));
        plan = getPlan(pattern);
        assertEquals(x.var(), plan.get(4).end());

        pattern = and(
                x.isa(superType),
                superType.type(thingy),
                y.isa(thingy2),
                subType.sub(superType),
                z.isa(subType),
                var().rel(x).rel(y));
        plan = getPlan(pattern);
        assertEquals(z.var(), plan.get(4).end());

        ((TransactionOLTP) tx).shard(tx.getEntityType(thingy1).id());
        ((TransactionOLTP) tx).shard(tx.getEntityType(thingy1).id());
        ((TransactionOLTP) tx).shard(tx.getEntityType(thingy).id());
        // now thing = 5, thing1 = 3

        pattern = and(
                x.isa(thingy),
                y.isa(thingy2),
                z.isa(thingy3),
                var().rel(x).rel(y).rel(z));
        plan = getPlan(pattern);
        assertEquals(y.var(), plan.get(3).end());

        pattern = and(
                x.isa(thingy1),
                y.isa(thingy2),
                z.isa(thingy3),
                var().rel(x).rel(y).rel(z));
        plan = getPlan(pattern);
        assertEquals(x.var(), plan.get(3).end());

        ((TransactionOLTP) tx).shard(tx.getEntityType(thingy1).id());
        ((TransactionOLTP) tx).shard(tx.getEntityType(thingy1).id());
        // now thing = 7, thing1 = 5

        pattern = and(
                x.isa(thingy),
                y.isa(thingy2),
                z.isa(thingy3),
                var().rel(x).rel(y).rel(z));
        plan = getPlan(pattern);
        assertEquals(y.var(), plan.get(3).end());

        pattern = and(
                x.isa(thingy1),
                y.isa(thingy2),
                z.isa(thingy3),
                var().rel(x).rel(y).rel(z));
        plan = getPlan(pattern);
        assertEquals(y.var(), plan.get(3).end());
    }

    private ImmutableList<Fragment> getPlan(Pattern pattern) {
        return GreedyTraversalPlan.createTraversal(pattern, ((TransactionOLTP) tx)).fragments().iterator().next();
    }
}