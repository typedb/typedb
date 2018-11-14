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

package grakn.core.test.graql.analytics;

import grakn.core.Session;
import grakn.core.Transaction;
import grakn.core.concept.AttributeType;
import grakn.core.concept.Entity;
import grakn.core.concept.EntityType;
import grakn.core.concept.Label;
import grakn.core.concept.RelationshipType;
import grakn.core.concept.Role;
import grakn.core.exception.GraqlQueryException;
import grakn.core.exception.InvalidKBException;
import grakn.core.test.rule.ConcurrentGraknServer;
import grakn.core.graql.internal.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static grakn.core.graql.Syntax.Compute.Algorithm.DEGREE;
import static grakn.core.graql.Syntax.Compute.Method.CENTRALITY;
import static grakn.core.graql.Syntax.Compute.Method.COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@SuppressWarnings("CheckReturnValue")
public class AnalyticsIT {

    private static final String thingy = "thingy";
    private static final String anotherThing = "anotherThing";
    private static final String related = "related";

    private String entityId1;
    private String entityId2;
    private String entityId3;
    private String entityId4;
    private String relationId12;
    private String relationId24;

    public Session session;

    @ClassRule
    public static final ConcurrentGraknServer server = new ConcurrentGraknServer();

    @Before
    public void setUp() {
        session = server.sessionWithNewKeyspace();
    }

    @After
    public void closeSession() { session.close(); }

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testNullResourceDoesNotBreakAnalytics() throws InvalidKBException {
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            // make slightly odd graph
            Label resourceTypeId = Label.of("degree");
            EntityType thingy = tx.putEntityType("thingy");

            AttributeType<Long> attribute = tx.putAttributeType(resourceTypeId, AttributeType.DataType.LONG);
            thingy.has(attribute);

            Role degreeOwner = tx.getRole(Schema.ImplicitType.HAS_OWNER.getLabel(resourceTypeId).getValue());
            Role degreeValue = tx.getRole(Schema.ImplicitType.HAS_VALUE.getLabel(resourceTypeId).getValue());
            RelationshipType relationshipType = tx.putRelationshipType(Schema.ImplicitType.HAS.getLabel(resourceTypeId))
                    .relates(degreeOwner)
                    .relates(degreeValue);
            thingy.plays(degreeOwner);

            Entity thisThing = thingy.create();
            relationshipType.create().assign(degreeOwner, thisThing);

            tx.commit();
        }

        // the null role-player caused analytics to fail at some stage
        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            tx.graql().compute(CENTRALITY).using(DEGREE).execute();
        } catch (RuntimeException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testSubgraphContainingRuleDoesNotBreakAnalytics() {
        expectedEx.expect(GraqlQueryException.class);
        expectedEx.expectMessage(GraqlQueryException.labelNotFound(Label.of("rule")).getMessage());
        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            tx.graql().compute(COUNT).in("rule", "thing").execute();
        }
    }

    @Test
    public void testSubgraphContainingRoleDoesNotBreakAnalytics() {
        expectedEx.expect(GraqlQueryException.class);
        expectedEx.expectMessage(GraqlQueryException.labelNotFound(Label.of("role")).getMessage());
        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            tx.graql().compute(COUNT).in("role").execute();
        }
    }

    @Test
    public void testConcurrentAnalyticsJobsBySubmittingGraqlComputeQueries() {
        addSchemaAndEntities();

        List<String> queryList = new ArrayList<>();
        queryList.add("compute count;");
        queryList.add("compute cluster using connected-component;");
        queryList.add("compute cluster using k-core;");
        queryList.add("compute centrality using degree;");
        queryList.add("compute centrality using k-core;");
        queryList.add("compute path from \"" + entityId1 + "\", to \"" + entityId4 + "\";");

        List<?> result = queryList.parallelStream().map(query -> {
            try (Transaction tx = session.transaction(Transaction.Type.READ)) {
                return tx.graql().parse(query).execute().toString();
            }
        }).collect(Collectors.toList());
        assertEquals(queryList.size(), result.size());
    }

    private void addSchemaAndEntities() throws InvalidKBException {
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            EntityType entityType1 = tx.putEntityType(thingy);
            EntityType entityType2 = tx.putEntityType(anotherThing);

            Entity entity1 = entityType1.create();
            Entity entity2 = entityType1.create();
            Entity entity3 = entityType1.create();
            Entity entity4 = entityType2.create();

            entityId1 = entity1.id().getValue();
            entityId2 = entity2.id().getValue();
            entityId3 = entity3.id().getValue();
            entityId4 = entity4.id().getValue();

            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            entityType1.plays(role1).plays(role2);
            entityType2.plays(role1).plays(role2);
            RelationshipType relationshipType = tx.putRelationshipType(related).relates(role1).relates(role2);

            relationId12 = relationshipType.create()
                    .assign(role1, entity1)
                    .assign(role2, entity2).id().getValue();
            relationId24 = relationshipType.create()
                    .assign(role1, entity2)
                    .assign(role2, entity4).id().getValue();

            tx.commit();
        }
    }
}
