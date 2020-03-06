/*
 * Copyright (C) 2020 Grakn Labs
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
import grakn.core.common.config.Config;
import grakn.core.graql.planning.gremlin.fragment.InIsaFragment;
import grakn.core.graql.planning.gremlin.fragment.LabelFragment;
import grakn.core.graql.planning.gremlin.fragment.OutIsaFragment;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Entity;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.graql.planning.gremlin.Fragment;
import grakn.core.kb.graql.planning.gremlin.TraversalPlanFactory;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.rule.GraknTestStorage;
import grakn.core.rule.SessionUtil;
import grakn.core.rule.TestTransactionProvider;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Statement;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static graql.lang.Graql.and;
import static graql.lang.Graql.var;
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
    public static GraknTestStorage storage = new GraknTestStorage();
    private static Session session;
    private Transaction tx;

    @BeforeClass
    public static void newSession() {
        Config mockServerConfig = storage.createCompatibleServerConfig();
        session = SessionUtil.serverlessSessionWithNewKeyspace(mockServerConfig);
        Transaction graph = session.writeTransaction();

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
        RelationType relationType1 = graph.putRelationType(related)
                .relates(role1).relates(role2).relates(role3);
        graph.putRelationType(sameAsRelated)
                .relates(role1).relates(role2).relates(role3);

        Role role4 = graph.putRole("role4");
        Role role5 = graph.putRole("role5");
        entityType1.plays(role4).plays(role5);
        entityType2.plays(role4).plays(role5);
        entityType4.plays(role4);
        graph.putRelationType(veryRelated)
                .relates(role4).relates(role5);

        Entity entity1 = entityType1.create();
        Entity entity2 = entityType2.create();
        Entity entity3 = entityType3.create();
        relationType1.create()
                .assign(role1, entity1)
                .assign(role2, entity2)
                .assign(role3, entity3);

        graph.commit();
    }

    @Before
    public void newTransaction() {
        tx = session.writeTransaction();
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
    public void inferUniqueRelationType() {
        Pattern pattern;
        ImmutableList<? extends Fragment> plan;

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

        // Relation type can now be inferred, so one more relation type label
        assertEquals(3L, plan.stream().filter(LabelFragment.class::isInstance).count());

        // Should start from the inferred relation type, instead of role players
        String relation = plan.get(1).start().name();
        assertNotEquals(relation, x.var().name());
        assertNotEquals(relation, y.var().name());
    }

    @Test
    public void inferRelationTypeWithMoreThan2Roles() {
        Pattern pattern;
        ImmutableList<? extends Fragment> plan;

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
        // Relation type can now be inferred, so one more relation type label
        assertEquals(4L, plan.stream().filter(LabelFragment.class::isInstance).count());
        assertEquals(4L, plan.stream()
                .filter(fragment -> fragment instanceof OutIsaFragment || fragment instanceof InIsaFragment).count());
    }

    @Test
    public void inferRelationTypeWithARolePlayerWithNoType() {
        Pattern pattern;
        ImmutableList<? extends Fragment> plan;

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
        // Relation type can now be inferred, so one more relation type label
        assertEquals(3L, plan.stream().filter(LabelFragment.class::isInstance).count());
        assertEquals(3L, plan.stream()
                .filter(fragment -> fragment instanceof OutIsaFragment || fragment instanceof InIsaFragment).count());
    }

    @Test
    public void inferRelationTypeWhereRolePlayedBySuperType() {
        Pattern pattern;
        ImmutableList<? extends Fragment> plan;

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
        // Relation type can now be inferred, so one more relation type label
        assertEquals(4L, plan.stream().filter(LabelFragment.class::isInstance).count());
        assertEquals(4L, plan.stream()
                .filter(fragment -> fragment instanceof OutIsaFragment || fragment instanceof InIsaFragment).count());
    }

    @Test
    public void inferRelationTypeWhereAVarHasTwoTypes() {
        Pattern pattern;
        ImmutableList<? extends Fragment> plan;

        pattern = and(
                x.isa(thingy),
                x.isa(thingy1),
                y.isa(thingy4),
                var().rel(x).rel(y));

        plan = getPlan(pattern);
        // Relation type can now be inferred, so one more relation type label plus existing 3 labels
        assertEquals(4L, plan.stream().filter(LabelFragment.class::isInstance).count());
        assertEquals(4L, plan.stream()
                .filter(fragment -> fragment instanceof OutIsaFragment || fragment instanceof InIsaFragment).count());

        // Should start from the inferred relation type, instead of role players
        String relation = plan.get(1).start().name();
        assertNotEquals(relation, x.var().name());
        assertNotEquals(relation, y.var().name());
    }

    @Test
    public void inferRelationTypeWhereAVarHasIncorrectTypes() {
        Pattern pattern;
        ImmutableList<? extends Fragment> plan;

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
    public void noIndexedStartNodeGeneratesValidPlans() {
        // a pattern without a indexed starting point so any of X, Y, Z,
        Pattern pattern = and(
                var().rel("role1", x).rel("role2", z),
                var().rel("role1", y).rel("role2", z)
        );
        // repeat planning step to handle nondeterminism, we may pick different starting points each time
        for (int i = 0; i < 20; i++) {
            ImmutableList<? extends Fragment> plan = getPlan(pattern);
            assertEquals(6, plan.size());
        }
    }

    @Test
    public void indexedFragmentsAreListedFirst() {
        Pattern pattern = and(
                x.isa(veryRelated),
                x.isa(thingy2),
                y.isa(thingy4),
                var().rel(x).rel(y),
                y.has(resourceType, "someString"));

        List<? extends Fragment> plan = getPlan(pattern);
        boolean priorFragmentHasFixedCost = true;
        for (Fragment fragment : plan) {
            if (!fragment.hasFixedFragmentCost()) {
                priorFragmentHasFixedCost = false;
            } else {
                // if the current fragment is fixed cost
                // and the prior one was, it's ok.
                // if it wasn't, fail the test
                assertTrue(priorFragmentHasFixedCost);
            }
        }
    }

    @Test
    public void avoidImplicitTypes() {
        /*
        Idea is: originally, the query planner could start at high priority starting nodes (non-implicit labels),
        low priority starting nodes (implicit labels which may represent edges instead of vertices), or worst case any
        valid node.

        This test ensures that we don't use implicit nodes as starting points if it can be avoided.
        In general implicit relations are non-reified and they correspond to an edge.
        */
        Pattern pattern = and(
                x.isa(thingy2),
                y.isa(thingy4),
                var().rel(x).rel(y));

        ImmutableList<? extends Fragment> plan = getPlan(pattern);
        assertEquals(3L, plan.stream().filter(LabelFragment.class::isInstance).count());
        List<? extends Fragment> nonLabelFragments = plan.stream().filter(f -> !(f instanceof LabelFragment)).collect(Collectors.toList());
        //first fragment after label fragments is an isa fragment so we skip it
        Fragment firstRolePlayerFragment = nonLabelFragments.get(1);
        String relationStartVarName = firstRolePlayerFragment.start().name();

        // should start from relation
        assertNotEquals(relationStartVarName, x.var().name());
        assertNotEquals(relationStartVarName, y.var().name());

        pattern = and(
                x.isa(resourceType),
                y.isa(thingy4),
                var().rel(x).rel(y));
        plan = getPlan(pattern);
        assertEquals(3L, plan.stream().filter(LabelFragment.class::isInstance).count());
        String relationEndVarName = firstRolePlayerFragment.end().name();

        // should start from a role player
        assertTrue(relationEndVarName.equals(x.var().name()) || relationEndVarName.equals(y.var().name()));
        //check next fragment after first role player fragment
        assertTrue(nonLabelFragments.get(nonLabelFragments.indexOf(firstRolePlayerFragment)+1) instanceof OutIsaFragment);
    }

    @Test
    public void sameLabelFragmentShouldNotBeAddedTwice() {
        Pattern pattern;
        ImmutableList<? extends Fragment> plan;

        pattern = and(
                x.isa(thingy2),
                y.isa(thingy4),
                z.isa(thingy2),
                var().rel(x).rel(y),
                var().rel(z).rel(y));
        plan = getPlan(pattern);

        // 2 thingy2 label, 1 thingy4, 1 inferred relation label
        assertEquals(4L, plan.stream().filter(LabelFragment.class::isInstance).count());

        // 5 isa fragments: x, y, z, relation between x and y, relation between z and y
        assertEquals(5L, plan.stream()
                .filter(fragment -> fragment instanceof OutIsaFragment || fragment instanceof InIsaFragment).count());
    }

    private ImmutableList<? extends Fragment> getPlan(Pattern pattern) {
        TestTransactionProvider.TestTransaction testTx = ((TestTransactionProvider.TestTransaction)tx);
        TraversalPlanFactory traversalPlanFactory = testTx.traversalPlanFactory();
        return traversalPlanFactory.createTraversal(pattern).fragments().iterator().next();
    }
}
