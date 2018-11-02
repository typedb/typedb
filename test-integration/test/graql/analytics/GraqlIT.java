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

package ai.grakn.test.graql.analytics;

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.Query;
import ai.grakn.graql.answer.ConceptList;
import ai.grakn.graql.answer.ConceptSet;
import ai.grakn.graql.answer.ConceptSetMeasure;
import ai.grakn.graql.answer.Value;
import ai.grakn.test.rule.GraknServer;
import ai.grakn.util.Schema;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.util.GraqlSyntax.Compute.Algorithm.CONNECTED_COMPONENT;
import static ai.grakn.util.GraqlSyntax.Compute.Argument.contains;
import static ai.grakn.util.GraqlSyntax.Compute.Method.CLUSTER;
import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("CheckReturnValue")
public class GraqlIT {

    private static final String thingy = "thingy";
    private static final String anotherThing = "anotherThing";
    private static final String related = "related";

    private String entityId1;
    private String entityId2;
    private String entityId3;
    private String entityId4;
    private String relationId12;
    private String relationId24;

    public GraknSession session;

    @ClassRule
    public static final GraknServer server = new GraknServer();

    @Before
    public void setUp() {
        session = server.sessionWithNewKeyspace();
    }

    @Test
    public void testGraqlCount() throws InvalidKBException {
        addSchemaAndEntities();
        try (GraknTx tx = session.transaction(GraknTxType.WRITE)) {
            assertEquals(6, tx.graql().<ComputeQuery<Value>>parse("compute count;")
                    .execute().get(0).number().intValue());

            assertEquals(3, tx.graql().<ComputeQuery<Value>>parse("compute count in [thingy, thingy];")
                    .execute().get(0).number().intValue());
        }
    }

    @Test
    public void testDegrees() {
        addSchemaAndEntities();
        try (GraknTx tx = session.transaction(GraknTxType.WRITE)) {
            List<ConceptSetMeasure> degrees =
                    tx.graql().<ComputeQuery<ConceptSetMeasure>>parse("compute centrality using degree;").execute();

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
        try (GraknTx tx = session.transaction(GraknTxType.WRITE)) {
            tx.graql().parse("compute sum of thingy;").execute();
        }
    }

    @Test(expected = GraqlQueryException.class)
    public void testInvalidTypeWithDegree() {
        try (GraknTx tx = session.transaction(GraknTxType.WRITE)) {
            tx.graql().parse("compute centrality of thingy, using degree;").execute();
        }
    }

    @Test
    public void testStatisticsMethods() throws InvalidKBException {
        try (GraknTx tx = session.transaction(GraknTxType.WRITE)) {
            Label resourceTypeId = Label.of("my-resource");

            AttributeType<Long> resource = tx.putAttributeType(resourceTypeId, AttributeType.DataType.LONG);
            EntityType thingy = tx.putEntityType("thingy");
            thingy.has(resource);

            Entity theResourceOwner = thingy.create();

            Role resourceOwner = tx.getRole(Schema.ImplicitType.HAS_OWNER.getLabel(resourceTypeId).getValue());
            Role resourceValue = tx.getRole(Schema.ImplicitType.HAS_VALUE.getLabel(resourceTypeId).getValue());
            RelationshipType relationshipType = tx.getRelationshipType(Schema.ImplicitType.HAS.getLabel(resourceTypeId).getValue());

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

        try (GraknTx tx = session.transaction(GraknTxType.WRITE)) {
            // use graql to compute various statistics
            Value result = tx.graql().<ComputeQuery<Value>>parse("compute sum of my-resource;").execute().get(0);
            assertEquals(6, result.number().intValue());
            result = tx.graql().<ComputeQuery<Value>>parse("compute min of my-resource;").execute().get(0);
            assertEquals(1, result.number().intValue());
            result = tx.graql().<ComputeQuery<Value>>parse("compute max of my-resource;").execute().get(0);
            assertEquals(3, result.number().intValue());
            result = tx.graql().<ComputeQuery<Value>>parse("compute mean of my-resource;").execute().get(0);
            assertNotNull(result.number());
            assertEquals(2.0, result.number().doubleValue(), 0.1);
            result = tx.graql().<ComputeQuery<Value>>parse("compute median of my-resource;").execute().get(0);
            assertEquals(2, result.number().intValue());
        }
    }

    @Test
    public void testConnectedComponents() throws InvalidKBException {
        try (GraknTx tx = session.transaction(GraknTxType.WRITE)) {
            List<ConceptSet> clusterList =
                    tx.graql().<ComputeQuery<ConceptSet>>parse("compute cluster using connected-component;").execute();
            assertTrue(clusterList.isEmpty());

            Query<?> parsed = tx.graql().parse("compute cluster using connected-component, where contains = V123;");
            Query<?> expected = tx.graql().compute(CLUSTER).using(CONNECTED_COMPONENT).where(contains(ConceptId.of("V123")));
            assertEquals(expected, parsed);
        }
    }

    @Test
    public void testSinglePath() throws InvalidKBException {
        addSchemaAndEntities();

        try (GraknTx tx = session.transaction(GraknTxType.WRITE)) {
            ComputeQuery<ConceptList> query = tx.graql().parse("compute path from '" + entityId1 + "', to '" + entityId2 + "';");
            List<ConceptList> paths = query.execute();

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

        try (GraknTx tx = session.transaction(GraknTxType.WRITE)) {
            ComputeQuery<ConceptList> query = tx.graql().parse("compute path from '" + entityId1 + "', to '" + entityId2 + "';");
            List<ConceptList> paths = query.execute();
            assertEquals(1, paths.size());
            List<String> result = paths.get(0).list().stream().map(ConceptId::getValue).collect(Collectors.toList());
            List<String> expected = Lists.newArrayList(entityId1, relationId12, entityId2);

            assertEquals(expected, result);
        }
    }

    @Test(expected = GraqlQueryException.class)
    public void testNonResourceTypeAsSubgraphForAnalytics() throws InvalidKBException {
        try (GraknTx tx = session.transaction(GraknTxType.WRITE)) {
            tx.putEntityType(thingy);
            tx.commit();
        }

        try (GraknTx tx = session.transaction(GraknTxType.WRITE)) {
            tx.graql().parse("compute sum of thingy;").execute();
        }
    }

    @Test(expected = GraqlQueryException.class)
    public void testErrorWhenNoSubgraphForAnalytics() throws InvalidKBException {
        try (GraknTx tx = session.transaction(GraknTxType.WRITE)) {
            tx.graql().parse("compute sum;").execute();
            tx.graql().parse("compute min;").execute();
            tx.graql().parse("compute max;").execute();
            tx.graql().parse("compute mean;").execute();
            tx.graql().parse("compute std;").execute();
        }
    }

    @Test
    public void testAnalyticsDoesNotCommitByMistake() throws InvalidKBException {
        try (GraknTx tx = session.transaction(GraknTxType.WRITE)) {
            tx.putAttributeType("number", AttributeType.DataType.LONG);
            tx.commit();
        }

        Set<String> analyticsCommands = new HashSet<>(Arrays.asList(
                "compute count;",
                "compute centrality using degree;",
                "compute mean of number;"));

        analyticsCommands.forEach(command -> {
            try (GraknTx tx = session.transaction(GraknTxType.WRITE)) {
                // insert a node but do not commit it
                tx.graql().parse("define thingy sub entity;").execute();
                // use analytics
                tx.graql().parse(command).execute();
            }

            try (GraknTx tx = session.transaction(GraknTxType.WRITE)) {
                // see if the node was commited
                assertNull(tx.getEntityType("thingy"));
            }
        });
    }

    private void addSchemaAndEntities() throws InvalidKBException {
        try (GraknTx tx = session.transaction(GraknTxType.WRITE)) {
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