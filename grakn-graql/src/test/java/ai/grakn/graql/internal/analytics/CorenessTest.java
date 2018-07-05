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
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.graql.Graql;
import ai.grakn.test.rule.SessionContext;
import ai.grakn.util.GraknTestUtil;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.util.GraqlSyntax.Compute.Algorithm.K_CORE;
import static ai.grakn.util.GraqlSyntax.Compute.Argument.min_k;
import static ai.grakn.util.GraqlSyntax.Compute.Method.CENTRALITY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

public class CorenessTest {
    private static final String thing = "thingy";
    private static final String anotherThing = "anotherThing";
    private static final String related = "related";
    private static final String veryRelated = "veryRelated";

    private ConceptId entityId1;
    private ConceptId entityId2;
    private ConceptId entityId3;
    private ConceptId entityId4;

    public GraknSession session;

    @ClassRule
    public final static SessionContext sessionContext = SessionContext.create();

    @Before
    public void setUp() {
        session = sessionContext.newSession();
    }

    @Test(expected = GraqlQueryException.class)
    public void testKSmallerThan2_ThrowsException() {
        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            graph.graql().compute(CENTRALITY).using(K_CORE).where(min_k(1)).execute();
        }
    }

    @Test
    public void testOnEmptyGraph_ReturnsEmptyMap() {
        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            Map<Long, Set<ConceptId>> result = graph.graql().compute(CENTRALITY).using(K_CORE).execute().getCentrality().get();
            assertTrue(result.isEmpty());
        }
    }

    @Test
    public void testOnGraphWithoutRelationships_ReturnsEmptyMap() {
        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            graph.putEntityType(thing).create();
            graph.putEntityType(anotherThing).create();
            Map<Long, Set<ConceptId>> result = graph.graql().compute(CENTRALITY).using(K_CORE).execute().getCentrality().get();
            assertTrue(result.isEmpty());
        }
    }

    @Test
    public void testOnGraphWithTwoEntitiesAndTwoRelationships() {
        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            EntityType entityType = graph.putEntityType(thing);
            Entity entity1 = entityType.create();
            Entity entity2 = entityType.create();

            Role role1 = graph.putRole("role1");
            Role role2 = graph.putRole("role2");
            entityType.play(role1).play(role2);
            graph.putRelationshipType(related)
                    .relate(role1).relate(role2)
                    .create()
                    .assign(role1, entity1)
                    .assign(role2, entity2);

            Role role3 = graph.putRole("role3");
            Role role4 = graph.putRole("role4");
            entityType.play(role3).play(role4);
            graph.putRelationshipType(veryRelated)
                    .relate(role3).relate(role4)
                    .create()
                    .assign(role3, entity1)
                    .assign(role4, entity2);

            Map<Long, Set<ConceptId>> result = graph.graql().compute(CENTRALITY).using(K_CORE).execute().getCentrality().get();
            assertTrue(result.isEmpty());
        }
    }

    @Test
    public void testOnGraphWithFourEntitiesAndSixRelationships() {
        addSchemaAndEntities();

        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            Map<Long, Set<ConceptId>> result = graph.graql().compute(CENTRALITY).using(K_CORE).execute().getCentrality().get();
            assertEquals(1, result.size());
            assertEquals(4, result.get(3L).size());

            result = graph.graql().compute(CENTRALITY).using(K_CORE).of(thing).execute().getCentrality().get();
            assertEquals(1, result.size());
            assertEquals(2, result.get(3L).size());

            result = graph.graql().compute(CENTRALITY).using(K_CORE).in(thing, anotherThing, related).execute().getCentrality().get();
            assertEquals(1, result.size());
            assertEquals(4, result.get(2L).size());
        }
    }

    @Test
    public void testImplicitTypeShouldBeIncluded() {
        addSchemaAndEntities();

        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            String aResourceTypeLabel = "aResourceTypeLabel";
            AttributeType<String> attributeType =
                    graph.putAttributeType(aResourceTypeLabel, AttributeType.DataType.STRING);
            graph.getEntityType(thing).has(attributeType);
            graph.getEntityType(anotherThing).has(attributeType);

            Attribute Attribute1 = attributeType.create("blah");
            graph.getConcept(entityId1).asEntity().has(Attribute1);
            graph.getConcept(entityId2).asEntity().has(Attribute1);
            graph.getConcept(entityId3).asEntity().has(Attribute1);
            graph.getConcept(entityId4).asEntity().has(Attribute1);

            Attribute Attribute2 = attributeType.create("bah");
            graph.getConcept(entityId1).asEntity().has(Attribute2);
            graph.getConcept(entityId2).asEntity().has(Attribute2);
            graph.getConcept(entityId3).asEntity().has(Attribute2);

            graph.commit();
        }

        Map<Long, Set<ConceptId>> result;
        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            result = graph.graql().compute(CENTRALITY).using(K_CORE).execute().getCentrality().get();
            System.out.println("result = " + result);
            assertEquals(2, result.size());
            assertEquals(5, result.get(4L).size());
            assertEquals(1, result.get(3L).size());

            result = graph.graql().compute(CENTRALITY).using(K_CORE).where(min_k(4L)).execute().getCentrality().get();
            assertEquals(1, result.size());
            assertEquals(5, result.get(4L).size());
        }
    }

    @Test
    public void testDisconnectedCores() {
        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            EntityType entityType1 = graph.putEntityType(thing);
            EntityType entityType2 = graph.putEntityType(anotherThing);

            Role role1 = graph.putRole("role1");
            Role role2 = graph.putRole("role2");
            RelationshipType relationshipType1 = graph.putRelationshipType(related)
                    .relate(role1).relate(role2);

            Role role3 = graph.putRole("role3");
            Role role4 = graph.putRole("role4");
            RelationshipType relationshipType2 = graph.putRelationshipType(veryRelated)
                    .relate(role3).relate(role4);

            entityType1.play(role1).play(role2).play(role3).play(role4);
            entityType2.play(role1).play(role2).play(role3).play(role4);

            Entity entity0 = entityType1.create();
            Entity entity1 = entityType1.create();
            Entity entity2 = entityType1.create();
            Entity entity3 = entityType1.create();
            Entity entity4 = entityType1.create();
            Entity entity5 = entityType1.create();
            Entity entity6 = entityType1.create();
            Entity entity7 = entityType1.create();
            Entity entity8 = entityType1.create();

            relationshipType1.create()
                    .assign(role1, entity1)
                    .assign(role2, entity2);
            relationshipType1.create()
                    .assign(role1, entity2)
                    .assign(role2, entity3);
            relationshipType1.create()
                    .assign(role1, entity3)
                    .assign(role2, entity4);
            relationshipType1.create()
                    .assign(role1, entity1)
                    .assign(role2, entity3);
            relationshipType1.create()
                    .assign(role1, entity1)
                    .assign(role2, entity4);
            relationshipType1.create()
                    .assign(role1, entity2)
                    .assign(role2, entity4);

            relationshipType1.create()
                    .assign(role1, entity5)
                    .assign(role2, entity6);
            relationshipType2.create()
                    .assign(role3, entity5)
                    .assign(role4, entity7);
            relationshipType2.create()
                    .assign(role3, entity5)
                    .assign(role4, entity8);
            relationshipType2.create()
                    .assign(role3, entity6)
                    .assign(role4, entity7);
            relationshipType2.create()
                    .assign(role3, entity6)
                    .assign(role4, entity8);
            relationshipType2.create()
                    .assign(role3, entity7)
                    .assign(role4, entity8);

            relationshipType1.create()
                    .assign(role1, entity0)
                    .assign(role2, entity1);
            relationshipType1.create()
                    .assign(role1, entity0)
                    .assign(role2, entity8);

            graph.commit();
        }

        Map<Long, Set<ConceptId>> result;
        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            result = graph.graql().compute(CENTRALITY).using(K_CORE).execute().getCentrality().get();
            assertEquals(2, result.size());
            assertEquals(8, result.get(3L).size());
            assertEquals(1, result.get(2L).size());

            result = graph.graql().compute(CENTRALITY).using(K_CORE).where(min_k(3L)).execute().getCentrality().get();
            assertEquals(1, result.size());
            assertEquals(8, result.get(3L).size());
        }
    }

    @Test
    public void testConcurrency() {
        assumeFalse(GraknTestUtil.usingTinker());

        addSchemaAndEntities();

        List<Long> list = new ArrayList<>(4);
        long workerNumber = 4L;
        for (long i = 0L; i < workerNumber; i++) {
            list.add(i);
        }

        Set<Map<Long, Set<ConceptId>>> result = list.parallelStream().map(i -> {
            try (GraknTx graph = session.transaction(GraknTxType.READ)) {
                return Graql.compute(CENTRALITY).withTx(graph).using(K_CORE).where(min_k(3L)).execute().getCentrality().get();
            }
        }).collect(Collectors.toSet());
        assertEquals(1, result.size());
        result.forEach(map -> {
            assertEquals(1, map.size());
            assertEquals(4, map.get(3L).size());
        });
    }

    private void addSchemaAndEntities() throws InvalidKBException {
        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            EntityType entityType1 = graph.putEntityType(thing);
            EntityType entityType2 = graph.putEntityType(anotherThing);

            Role role1 = graph.putRole("role1");
            Role role2 = graph.putRole("role2");
            RelationshipType relationshipType1 = graph.putRelationshipType(related)
                    .relate(role1).relate(role2);

            Role role3 = graph.putRole("role3");
            Role role4 = graph.putRole("role4");
            RelationshipType relationshipType2 = graph.putRelationshipType(veryRelated)
                    .relate(role3).relate(role4);

            entityType1.play(role1).play(role2).play(role3).play(role4);
            entityType2.play(role1).play(role2).play(role3).play(role4);

            Entity entity1 = entityType1.create();
            Entity entity2 = entityType1.create();
            Entity entity3 = entityType2.create();
            Entity entity4 = entityType2.create();

            relationshipType1.create()
                    .assign(role1, entity1)
                    .assign(role2, entity2);
            relationshipType1.create()
                    .assign(role1, entity2)
                    .assign(role2, entity3);
            relationshipType1.create()
                    .assign(role1, entity3)
                    .assign(role2, entity4);
            relationshipType1.create()
                    .assign(role1, entity4)
                    .assign(role2, entity1);

            relationshipType2.create()
                    .assign(role3, entity1)
                    .assign(role4, entity3);
            relationshipType2.create()
                    .assign(role3, entity2)
                    .assign(role4, entity4);

            entityId1 = entity1.id();
            entityId2 = entity2.id();
            entityId3 = entity3.id();
            entityId4 = entity4.id();

            graph.commit();
        }
    }
}
