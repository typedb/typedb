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

package ai.grakn.graql.internal.analytics;

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
import ai.grakn.test.rule.SessionContext;
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
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.util.GraqlSyntax.Compute.Algorithm.CONNECTED_COMPONENT;
import static ai.grakn.util.GraqlSyntax.Compute.Argument.contains;
import static ai.grakn.util.GraqlSyntax.Compute.Method.CLUSTER;
import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GraqlTest {

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
    public final static SessionContext sessionContext = SessionContext.create();

    @Before
    public void setUp() {
        session = sessionContext.newSession();
    }

    @Test
    public void testGraqlCount() throws InvalidKBException {
        addSchemaAndEntities();
        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            assertEquals(6L,
                    ((ComputeQuery.Answer) (graph.graql().parse("compute count;").execute())).getNumber().get());
            assertEquals(3L,
                    ((ComputeQuery.Answer) graph.graql().parse("compute count in [thingy, thingy];").execute()).getNumber().get());
        }
    }

    @Test
    public void testDegrees() {
        addSchemaAndEntities();
        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            Map<Long, Set<ConceptId>> degrees =
                    graph.graql().<ComputeQuery>parse("compute centrality using degree;").execute().getCentrality().get();

            Map<String, Long> correctDegrees = new HashMap<>();
            correctDegrees.put(entityId1, 1L);
            correctDegrees.put(entityId2, 2L);
            correctDegrees.put(entityId3, 0L);
            correctDegrees.put(entityId4, 1L);
            correctDegrees.put(relationId12, 2L);
            correctDegrees.put(relationId24, 2L);

            assertTrue(!degrees.isEmpty());
            degrees.forEach((key, value) -> value.forEach(
                    id -> {
                        assertTrue(correctDegrees.containsKey(id.getValue()));
                        assertEquals(correctDegrees.get(id.getValue()), key);
                    }
            ));
        }
    }

    @Test(expected = GraqlQueryException.class)
    public void testInvalidTypeWithStatistics() {
        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            graph.graql().parse("compute sum of thingy;").execute();
        }
    }

    @Test(expected = GraqlQueryException.class)
    public void testInvalidTypeWithDegree() {
        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            graph.graql().parse("compute centrality of thingy, using degree;").execute();
        }
    }

    @Test
    public void testStatisticsMethods() throws InvalidKBException {
        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            Label resourceTypeId = Label.of("my-resource");

            AttributeType<Long> resource = graph.putAttributeType(resourceTypeId, AttributeType.DataType.LONG);
            EntityType thingy = graph.putEntityType("thingy");
            thingy.attribute(resource);

            Entity theResourceOwner = thingy.addEntity();

            Role resourceOwner = graph.getRole(Schema.ImplicitType.HAS_OWNER.getLabel(resourceTypeId).getValue());
            Role resourceValue = graph.getRole(Schema.ImplicitType.HAS_VALUE.getLabel(resourceTypeId).getValue());
            RelationshipType relationshipType = graph.getRelationshipType(Schema.ImplicitType.HAS.getLabel(resourceTypeId).getValue());

            relationshipType.addRelationship()
                    .addRolePlayer(resourceOwner, theResourceOwner)
                    .addRolePlayer(resourceValue, resource.putAttribute(1L));
            relationshipType.addRelationship()
                    .addRolePlayer(resourceOwner, theResourceOwner)
                    .addRolePlayer(resourceValue, resource.putAttribute(2L));
            relationshipType.addRelationship()
                    .addRolePlayer(resourceOwner, theResourceOwner)
                    .addRolePlayer(resourceValue, resource.putAttribute(3L));

            graph.commit();
        }

        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            // use graql to compute various statistics
            Optional<? extends Number> result =
                    graph.graql().<ComputeQuery>parse("compute sum of my-resource;").execute().getNumber();
            assertEquals(Optional.of(6L), result);
            result = graph.graql().<ComputeQuery>parse("compute min of my-resource;").execute().getNumber();
            assertEquals(Optional.of(1L), result);
            result = graph.graql().<ComputeQuery>parse("compute max of my-resource;").execute().getNumber();
            assertEquals(Optional.of(3L), result);
            result = graph.graql().<ComputeQuery>parse("compute mean of my-resource;").execute().getNumber();
            assert result.isPresent();
            assertEquals(2.0, (Double) result.get(), 0.1);
            result = graph.graql().<ComputeQuery>parse("compute median of my-resource;").execute().getNumber();
            assertEquals(Optional.of(2L), result);
        }
    }

    @Test
    public void testConnectedComponents() throws InvalidKBException {
        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            List<Long> sizeList =
                    graph.graql().<ComputeQuery>parse("compute cluster using connected-component;").execute().getClusterSizes().get();
            assertTrue(sizeList.isEmpty());
            Set<Set<ConceptId>> membersList = graph.graql().<ComputeQuery>parse(
                    "compute cluster using connected-component, where members = true;").execute().getClusters().get();
            assertTrue(membersList.isEmpty());

            Query<?> parsed = graph.graql().parse(
                    "compute cluster using connected-component, where contains = V123;");
            Query<?> expected = graph.graql().compute(CLUSTER).using(CONNECTED_COMPONENT).where(contains(ConceptId.of("V123")));
            assertEquals(expected, parsed);
        }
    }

    @Test
    public void testSinglePath() throws InvalidKBException {
        addSchemaAndEntities();

        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            ComputeQuery query = graph.graql().parse("compute path from '" + entityId1 + "', to '" + entityId2 + "';");
            List<List<ConceptId>> paths = query.execute().getPaths().get();

            List<ConceptId> path = Collections.emptyList();
            if (!paths.isEmpty()) path = paths.get(0);
            List<String> result = path.stream().map(ConceptId::getValue).collect(Collectors.toList());
            List<String> expected = Lists.newArrayList(entityId1, relationId12, entityId2);

            assertEquals(expected, result);
        }
    }

    @Test
    public void testPath() throws InvalidKBException {
        addSchemaAndEntities();

        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            ComputeQuery query = graph.graql().parse("compute path from '" + entityId1 + "', to '" + entityId2 + "';");

            List<List<ConceptId>> path = query.execute().getPaths().get();
            assertEquals(1, path.size());
            List<String> result = path.get(0).stream().map(ConceptId::getValue).collect(Collectors.toList());
            List<String> expected = Lists.newArrayList(entityId1, relationId12, entityId2);

            assertEquals(expected, result);
        }
    }

    @Test(expected = GraqlQueryException.class)
    public void testNonResourceTypeAsSubgraphForAnalytics() throws InvalidKBException {
        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            graph.putEntityType(thingy);
            graph.commit();
        }

        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            graph.graql().parse("compute sum of thingy;").execute();
        }
    }

    @Test(expected = GraqlQueryException.class)
    public void testErrorWhenNoSubgraphForAnalytics() throws InvalidKBException {
        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            graph.graql().parse("compute sum;").execute();
            graph.graql().parse("compute min;").execute();
            graph.graql().parse("compute max;").execute();
            graph.graql().parse("compute mean;").execute();
            graph.graql().parse("compute std;").execute();
        }
    }

    @Test
    public void testAnalyticsDoesNotCommitByMistake() throws InvalidKBException {
        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            graph.putAttributeType("number", AttributeType.DataType.LONG);
            graph.commit();
        }

        Set<String> analyticsCommands = new HashSet<>(Arrays.asList(
                "compute count;",
                "compute centrality using degree;",
                "compute mean of number;"));

        analyticsCommands.forEach(command -> {
            try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
                // insert a node but do not commit it
                graph.graql().parse("define thingy sub entity;").execute();
                // use analytics
                graph.graql().parse(command).execute();
            }

            try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
                // see if the node was commited
                assertNull(graph.getEntityType("thingy"));
            }
        });
    }

    private void addSchemaAndEntities() throws InvalidKBException {
        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            EntityType entityType1 = graph.putEntityType(thingy);
            EntityType entityType2 = graph.putEntityType(anotherThing);

            Entity entity1 = entityType1.addEntity();
            Entity entity2 = entityType1.addEntity();
            Entity entity3 = entityType1.addEntity();
            Entity entity4 = entityType2.addEntity();

            entityId1 = entity1.getId().getValue();
            entityId2 = entity2.getId().getValue();
            entityId3 = entity3.getId().getValue();
            entityId4 = entity4.getId().getValue();

            Role role1 = graph.putRole("role1");
            Role role2 = graph.putRole("role2");
            entityType1.plays(role1).plays(role2);
            entityType2.plays(role1).plays(role2);
            RelationshipType relationshipType = graph.putRelationshipType(related).relates(role1).relates(role2);

            relationId12 = relationshipType.addRelationship()
                    .addRolePlayer(role1, entity1)
                    .addRolePlayer(role2, entity2).getId().getValue();
            relationId24 = relationshipType.addRelationship()
                    .addRolePlayer(role1, entity2)
                    .addRolePlayer(role2, entity4).getId().getValue();

            graph.commit();
        }
    }
}
