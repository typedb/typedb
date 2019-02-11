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

package grakn.core.graql.analytics;

import com.google.common.collect.Lists;
import grakn.core.graql.answer.ConceptList;
import grakn.core.graql.answer.ConceptSet;
import grakn.core.graql.answer.ConceptSetMeasure;
import grakn.core.graql.answer.Value;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Entity;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.RelationType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.Schema;
import grakn.core.graql.query.query.GraqlCompute;
import grakn.core.graql.query.Graql;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
import grakn.core.server.exception.InvalidKBException;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static graql.lang.util.Token.Compute.Algorithm.CONNECTED_COMPONENT;
import static graql.lang.util.Token.Compute.Algorithm.DEGREE;
import static grakn.core.graql.query.query.GraqlCompute.Argument.contains;
import static grakn.core.graql.query.query.GraqlCompute.Method.CENTRALITY;
import static grakn.core.graql.query.query.GraqlCompute.Method.CLUSTER;
import static grakn.core.graql.query.query.GraqlCompute.Method.COUNT;
import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings("CheckReturnValue")
public class GraqlComputeIT {

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
    public static final GraknTestServer server = new GraknTestServer();

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
            RelationType relationshipType = tx.putRelationshipType(Schema.ImplicitType.HAS.getLabel(resourceTypeId))
                    .relates(degreeOwner)
                    .relates(degreeValue);
            thingy.plays(degreeOwner);

            Entity thisThing = thingy.create();
            relationshipType.create().assign(degreeOwner, thisThing);

            tx.commit();
        }

        // the null role-player caused analytics to fail at some stage
        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            tx.execute(Graql.compute(CENTRALITY).using(DEGREE));
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
            tx.execute(Graql.compute(COUNT).in("rule", "thing"));
        }
    }

    @Test
    public void testSubgraphContainingRoleDoesNotBreakAnalytics() {
        expectedEx.expect(GraqlQueryException.class);
        expectedEx.expectMessage(GraqlQueryException.labelNotFound(Label.of("role")).getMessage());
        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            tx.execute(Graql.compute(COUNT).in("role"));
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
                return tx.execute(Graql.parse(query).asCompute()).toString();
            }
        }).collect(Collectors.toList());
        assertEquals(queryList.size(), result.size());
    }

    @Test
    public void testGraqlCount() throws InvalidKBException {
        addSchemaAndEntities();
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            assertEquals(6, tx.execute(Graql.<GraqlCompute<Value>>parse("compute count;"))
                    .get(0).number().intValue());

            assertEquals(3, tx.execute(Graql.<GraqlCompute<Value>>parse("compute count in [thingy, thingy];"))
                    .get(0).number().intValue());
        }
    }

    @Test
    public void testDegrees() {
        addSchemaAndEntities();
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            List<ConceptSetMeasure> degrees =
                    tx.execute(Graql.<GraqlCompute<ConceptSetMeasure>>parse("compute centrality using degree;"));

            Map<String, Long> correctDegrees = new HashMap<>();
            correctDegrees.put(entityId1, 1L);
            correctDegrees.put(entityId2, 2L);
            correctDegrees.put(entityId3, 0L);
            correctDegrees.put(entityId4, 1L);
            correctDegrees.put(relationId12, 2L);
            correctDegrees.put(relationId24, 2L);

            assertTrue(!degrees.isEmpty());
            degrees.forEach(conceptSetMeasure -> conceptSetMeasure.set().forEach(
                    id -> {
                        assertTrue(correctDegrees.containsKey(id.getValue()));
                        assertEquals(correctDegrees.get(id.getValue()).intValue(), conceptSetMeasure.measurement().intValue());
                    }
            ));
        }
    }

    @Test(expected = GraqlQueryException.class)
    public void testInvalidTypeWithStatistics() {
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            tx.execute(Graql.parse("compute sum of thingy;").asCompute());
        }
    }

    @Test(expected = GraqlQueryException.class)
    public void testInvalidTypeWithDegree() {
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            tx.execute(Graql.parse("compute centrality of thingy, using degree;").asCompute());
        }
    }

    @Test
    public void testStatisticsMethods() throws InvalidKBException {
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            Label resourceTypeId = Label.of("my-resource");

            AttributeType<Long> resource = tx.putAttributeType(resourceTypeId, AttributeType.DataType.LONG);
            EntityType thingy = tx.putEntityType("thingy");
            thingy.has(resource);

            Entity theResourceOwner = thingy.create();

            Role resourceOwner = tx.getRole(Schema.ImplicitType.HAS_OWNER.getLabel(resourceTypeId).getValue());
            Role resourceValue = tx.getRole(Schema.ImplicitType.HAS_VALUE.getLabel(resourceTypeId).getValue());
            RelationType relationshipType = tx.getRelationshipType(Schema.ImplicitType.HAS.getLabel(resourceTypeId).getValue());

            relationshipType.create()
                    .assign(resourceOwner, theResourceOwner)
                    .assign(resourceValue, resource.create(1L));
            relationshipType.create()
                    .assign(resourceOwner, theResourceOwner)
                    .assign(resourceValue, resource.create(2L));
            relationshipType.create()
                    .assign(resourceOwner, theResourceOwner)
                    .assign(resourceValue, resource.create(3L));

            tx.commit();
        }

        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            // use graql to compute various statistics
            Value result = tx.execute(Graql.<GraqlCompute<Value>>parse("compute sum of my-resource;")).get(0);
            assertEquals(6, result.number().intValue());
            result = tx.execute(Graql.<GraqlCompute<Value>>parse("compute min of my-resource;")).get(0);
            assertEquals(1, result.number().intValue());
            result = tx.execute(Graql.<GraqlCompute<Value>>parse("compute max of my-resource;")).get(0);
            assertEquals(3, result.number().intValue());
            result = tx.execute(Graql.<GraqlCompute<Value>>parse("compute mean of my-resource;")).get(0);
            assertNotNull(result.number());
            assertEquals(2.0, result.number().doubleValue(), 0.1);
            result = tx.execute(Graql.<GraqlCompute<Value>>parse("compute median of my-resource;")).get(0);
            assertEquals(2, result.number().intValue());
        }
    }

    @Test
    public void testConnectedComponents() throws InvalidKBException {
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            List<ConceptSet> clusterList =
                    tx.execute(Graql.<GraqlCompute<ConceptSet>>parse("compute cluster using connected-component;"));
            assertTrue(clusterList.isEmpty());

            GraqlCompute<?> parsed = Graql.parse("compute cluster using connected-component, where contains = V123;").asCompute();
            GraqlCompute<?> expected = Graql.compute(CLUSTER).using(CONNECTED_COMPONENT).where(contains(ConceptId.of("V123")));
            assertEquals(expected, parsed);
        }
    }

    @Test
    public void testSinglePath() throws InvalidKBException {
        addSchemaAndEntities();

        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            GraqlCompute<ConceptList> query = Graql.parse("compute path from '" + entityId1 + "', to '" + entityId2 + "';");
            List<ConceptList> paths = tx.execute(query);

            List<ConceptId> path = Collections.emptyList();
            if (!paths.isEmpty()) path = paths.get(0).list();
            List<String> result = path.stream().map(ConceptId::getValue).collect(Collectors.toList());
            List<String> expected = Lists.newArrayList(entityId1, relationId12, entityId2);

            assertEquals(expected, result);
        }
    }

    @Test
    public void testPath() throws InvalidKBException {
        addSchemaAndEntities();

        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            GraqlCompute<ConceptList> query = Graql.parse("compute path from '" + entityId1 + "', to '" + entityId2 + "';");
            List<ConceptList> paths = tx.execute(query);
            assertEquals(1, paths.size());
            List<String> result = paths.get(0).list().stream().map(ConceptId::getValue).collect(Collectors.toList());
            List<String> expected = Lists.newArrayList(entityId1, relationId12, entityId2);

            assertEquals(expected, result);
        }
    }

    @Test(expected = GraqlQueryException.class)
    public void testNonResourceTypeAsSubgraphForAnalytics() throws InvalidKBException {
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            tx.putEntityType(thingy);
            tx.commit();
        }

        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            tx.execute(Graql.parse("compute sum of thingy;").asCompute());
        }
    }

    @Test(expected = GraqlQueryException.class)
    public void testErrorWhenNoSubgraphForAnalytics() throws InvalidKBException {
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            tx.execute(Graql.parse("compute sum;").asCompute());
            tx.execute(Graql.parse("compute min;").asCompute());
            tx.execute(Graql.parse("compute max;").asCompute());
            tx.execute(Graql.parse("compute mean;").asCompute());
            tx.execute(Graql.parse("compute std;").asCompute());
        }
    }

    @Test
    public void testAnalyticsDoesNotCommitByMistake() throws InvalidKBException {
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            tx.putAttributeType("number", AttributeType.DataType.LONG);
            tx.commit();
        }

        Set<String> analyticsCommands = new HashSet<>(Arrays.asList(
                "compute count;",
                "compute centrality using degree;",
                "compute mean of number;"));

        analyticsCommands.forEach(command -> {
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                // insert a node but do not commit it
                tx.execute(Graql.parse("define thingy sub entity;").asDefine());
                // use analytics
                tx.execute(Graql.parse(command).asCompute());
            }

            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                // see if the node was commited
                assertNull(tx.getEntityType("thingy"));
            }
        });
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
            RelationType relationshipType = tx.putRelationshipType(related).relates(role1).relates(role2);

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
