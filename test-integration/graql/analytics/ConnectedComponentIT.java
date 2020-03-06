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

package grakn.core.graql.analytics;

import com.google.common.collect.Sets;
import grakn.core.concept.answer.ConceptSet;
import grakn.core.core.Schema;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.Entity;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.graql.exception.GraqlSemanticException;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.kb.server.exception.InvalidKBException;
import grakn.core.rule.GraknTestServer;
import graql.lang.Graql;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static graql.lang.Graql.Token.Compute.Algorithm.CONNECTED_COMPONENT;
import static graql.lang.query.GraqlCompute.Argument.contains;
import static graql.lang.query.GraqlCompute.Argument.size;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings({"CheckReturnValue", "Duplicates"})
public class ConnectedComponentIT {
    private static final String thing = "thingy";
    private static final String anotherThing = "anotherThing";
    private static final String related = "related";

    private static final String resourceType1 = "resourceType1";
    private static final String resourceType2 = "resourceType2";
    private static final String resourceType3 = "resourceType3";
    private static final String resourceType4 = "resourceType4";
    private static final String resourceType5 = "resourceType5";
    private static final String resourceType6 = "resourceType6";
    private static final String resourceType7 = "resourceType7";

    private ConceptId entityId1;
    private ConceptId entityId2;
    private ConceptId entityId3;
    private ConceptId entityId4;
    private ConceptId aDisconnectedAttribute;

    public Session session;

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    @Before
    public void setUp() {
        session = server.sessionWithNewKeyspace();
    }

    @After
    public void closeSession() { session.close(); }

    @Test
    public void testNullSourceIdIsIgnored() {
        try (Transaction tx = session.readTransaction()) {
            tx.execute(Graql.compute().cluster().using(CONNECTED_COMPONENT).where(contains(null)));
        }

        addSchemaAndEntities();
        try (Transaction tx = session.readTransaction()) {
            tx.execute(Graql.compute().cluster().using(CONNECTED_COMPONENT).in(thing).where(contains(null)));
        }
    }

    @Test(expected = GraqlSemanticException.class)
    public void testSourceDoesNotExistInSubGraph() {
        addSchemaAndEntities();
        try (Transaction tx = session.readTransaction()) {
            tx.execute(Graql.compute().cluster().using(CONNECTED_COMPONENT).in(thing).where(contains(entityId4.getValue())));
        }
    }

    @Test
    public void testConnectedComponentOnEmptyGraph() {
        try (Transaction tx = session.writeTransaction()) {
            // test on an empty rule.tx()
            List<ConceptSet> clusterList = tx.execute(Graql.compute().cluster().using(CONNECTED_COMPONENT).attributes(true));
            assertTrue(clusterList.isEmpty());
            clusterList = tx.execute(Graql.compute().cluster().using(CONNECTED_COMPONENT));
            assertTrue(clusterList.isEmpty());
            assertEquals(0, tx.execute(Graql.compute().count()).get(0).number().intValue());
        }
    }

    @Test
    public void testConnectedComponentSize() {
        List<ConceptSet> clusterList;

        addSchemaAndEntities();

        try (Transaction tx = session.writeTransaction()) {
            clusterList = tx.execute(Graql.compute().cluster().using(CONNECTED_COMPONENT).attributes(true).where(size(1L)));
            assertEquals(0, clusterList.size());

            clusterList = tx.execute(Graql.compute().cluster().using(CONNECTED_COMPONENT).where(size(1L)));
            assertEquals(0, clusterList.size());
        }

        addResourceRelations();

        try (Transaction tx = session.readTransaction()) {
            clusterList = tx.execute(Graql.compute().cluster().using(CONNECTED_COMPONENT).attributes(true).where(size(1L)));
            assertEquals(5, clusterList.size());

            clusterList = tx.execute(Graql.compute().cluster().using(CONNECTED_COMPONENT).attributes(true).where(size(1L), contains(entityId1.getValue())));
            assertEquals(0, clusterList.size());

            clusterList = tx.execute(Graql.compute().cluster().using(CONNECTED_COMPONENT).where(size(1L)));
            assertEquals(0, clusterList.size());

            clusterList = tx.execute(Graql.compute().cluster().using(CONNECTED_COMPONENT).where(contains(entityId4.getValue()), size(1L)));
            assertEquals(0, clusterList.size());
        }
    }

    @Test
    public void testConnectedComponentImplicitType() {
        String aResourceTypeLabel = "aResourceTypeLabel";

        addSchemaAndEntities();
        addResourceRelations();

        try (Transaction tx = session.writeTransaction()) {
            AttributeType<String> attributeType =
                    tx.putAttributeType(aResourceTypeLabel, AttributeType.DataType.STRING);
            tx.getEntityType(thing).has(attributeType);
            tx.getEntityType(anotherThing).has(attributeType);
            Attribute aAttribute = attributeType.create("blah");
            tx.getEntityType(thing).instances().forEach(instance -> instance.has(aAttribute));
            tx.getEntityType(anotherThing).instances().forEach(instance -> instance.has(aAttribute));
            tx.commit();
        }

        try (Transaction tx = session.readTransaction()) {
            List<ConceptSet> clusterList = tx.execute(Graql.compute().cluster().using(CONNECTED_COMPONENT)
                    .in(thing, anotherThing, aResourceTypeLabel, Schema.ImplicitType.HAS.getLabel(aResourceTypeLabel).getValue()));
            assertEquals(1, clusterList.size());
            assertEquals(5, clusterList.iterator().next().set().size());

            clusterList = tx.execute(Graql.compute().cluster().using(CONNECTED_COMPONENT)
                    .in(thing, anotherThing, aResourceTypeLabel, Schema.ImplicitType.HAS.getLabel(aResourceTypeLabel).getValue())
                    .where(contains(entityId2.getValue())));
            assertEquals(1, clusterList.size());
            assertEquals(5, clusterList.iterator().next().set().size());

            assertEquals(1, tx.execute(Graql.compute().cluster().using(CONNECTED_COMPONENT).attributes(true)
                    .in(thing, anotherThing, aResourceTypeLabel, Schema.ImplicitType.HAS.getLabel(aResourceTypeLabel).getValue())
                    .attributes(true)).size());
        }
    }

    @Test
    public void testConnectedComponent() {
        List<ConceptSet> clusterList;

        addSchemaAndEntities();

        try (Transaction tx = session.readTransaction()) {
            clusterList = tx.execute(Graql.compute().cluster().using(CONNECTED_COMPONENT).attributes(true));
            assertEquals(1, clusterList.size());
            assertEquals(7, clusterList.iterator().next().set().size()); // 4 entities, 3 assertions

            clusterList = tx.execute(Graql.compute().cluster().using(CONNECTED_COMPONENT).where(contains(entityId1.getValue())).attributes(true));
            assertEquals(1, clusterList.size());
            assertEquals(7, clusterList.iterator().next().set().size());

            clusterList = tx.execute(Graql.compute().cluster().using(CONNECTED_COMPONENT));
            assertEquals(1, clusterList.size());
            assertEquals(7, clusterList.iterator().next().set().size());

            clusterList = tx.execute(Graql.compute().cluster().using(CONNECTED_COMPONENT).where(contains(entityId4.getValue())));
            assertEquals(1, clusterList.size());
            assertEquals(7, clusterList.iterator().next().set().size());
        }

        // add different resources. This may change existing cluster labels.
        addResourceRelations();

        try (Transaction tx = session.readTransaction()) {
            clusterList = tx.execute(Graql.compute().cluster().using(CONNECTED_COMPONENT).attributes(true));
            Map<Integer, Integer> populationCount00 = new HashMap<>();
            clusterList.forEach(cluster -> populationCount00.put(cluster.set().size(),
                    populationCount00.containsKey(cluster.set().size()) ? populationCount00.get(cluster.set().size()) + 1 : 1));
            // 5 resources are not connected to anything
            assertEquals(5, populationCount00.get(1).intValue());
            assertEquals(1, populationCount00.get(15).intValue());

            clusterList = tx.execute(Graql.compute().cluster().using(CONNECTED_COMPONENT).where(contains(aDisconnectedAttribute.getValue())).attributes(true));
            assertEquals(1, clusterList.size());

            clusterList = tx.execute(Graql.compute().cluster().using(CONNECTED_COMPONENT));
            assertEquals(1, clusterList.size());
            Map<Integer, Integer> populationCount1 = new HashMap<>();
            clusterList.forEach(cluster -> populationCount1.put(cluster.set().size(),
                    populationCount1.containsKey(cluster.set().size()) ? populationCount1.get(cluster.set().size()) + 1 : 1));
            assertEquals(1, populationCount1.get(7).intValue());

            // test on subtypes. This will change existing cluster labels.
            Set<String> subTypes = Sets.newHashSet(thing, anotherThing, resourceType1, resourceType2,
                    resourceType3, resourceType4, resourceType5, resourceType6);
            clusterList = tx.execute(Graql.compute().cluster().using(CONNECTED_COMPONENT).in(subTypes));
            assertEquals(17, clusterList.size()); // No relations, so this is the entity count;
        }
    }

    @Test
    public void testConnectedComponentConcurrency() {
        addSchemaAndEntities();
        addResourceRelations();

        List<Long> list = new ArrayList<>(4);
        long workerNumber = 4L;
        for (long i = 0L; i < workerNumber; i++) {
            list.add(i);
        }

        Set<List<ConceptSet>> result = list.parallelStream().map(i -> {
            try (Transaction tx = session.readTransaction()) {
                return tx.execute(Graql.compute().cluster().using(CONNECTED_COMPONENT));
            }
        }).collect(Collectors.toSet());
        result.forEach(map -> {
            assertEquals(1, map.size());
            assertEquals(7L, map.iterator().next().set().size());
        });
    }

    private void addSchemaAndEntities() throws InvalidKBException {
        try (Transaction tx = session.writeTransaction()) {

            EntityType entityType1 = tx.putEntityType(thing);
            EntityType entityType2 = tx.putEntityType(anotherThing);

            Entity entity1 = entityType1.create();
            Entity entity2 = entityType1.create();
            Entity entity3 = entityType1.create();
            Entity entity4 = entityType2.create();
            entityId1 = entity1.id();
            entityId2 = entity2.id();
            entityId3 = entity3.id();
            entityId4 = entity4.id();

            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            entityType1.plays(role1).plays(role2);
            entityType2.plays(role1).plays(role2);
            RelationType relationType = tx.putRelationType(related).relates(role1).relates(role2);

            relationType.create()
                    .assign(role1, entity1)
                    .assign(role2, entity2).id();
            relationType.create()
                    .assign(role1, entity2)
                    .assign(role2, entity3).id();
            relationType.create()
                    .assign(role1, entity2)
                    .assign(role2, entity4).id();

            List<AttributeType> attributeTypeList = new ArrayList<>();
            attributeTypeList.add(tx.putAttributeType(resourceType1, AttributeType.DataType.DOUBLE));
            attributeTypeList.add(tx.putAttributeType(resourceType2, AttributeType.DataType.LONG));
            attributeTypeList.add(tx.putAttributeType(resourceType3, AttributeType.DataType.LONG));
            attributeTypeList.add(tx.putAttributeType(resourceType4, AttributeType.DataType.STRING));
            attributeTypeList.add(tx.putAttributeType(resourceType5, AttributeType.DataType.LONG));
            attributeTypeList.add(tx.putAttributeType(resourceType6, AttributeType.DataType.DOUBLE));
            attributeTypeList.add(tx.putAttributeType(resourceType7, AttributeType.DataType.DOUBLE));

            attributeTypeList.forEach(attributeType -> {
                entityType1.has(attributeType);
                entityType2.has(attributeType);
            });

            tx.commit();
        }
    }

    private void addResourceRelations() throws InvalidKBException {
        try (Transaction tx = session.writeTransaction()) {

            Entity entity1 = tx.getConcept(entityId1);
            Entity entity2 = tx.getConcept(entityId2);
            Entity entity3 = tx.getConcept(entityId3);
            Entity entity4 = tx.getConcept(entityId4);

            entity1.has(tx.getAttributeType(resourceType1).create(1.2))
                    .has(tx.getAttributeType(resourceType1).create(1.5))
                    .has(tx.getAttributeType(resourceType2).create(4L))
                    .has(tx.getAttributeType(resourceType2).create(-1L))
                    .has(tx.getAttributeType(resourceType5).create(-7L))
                    .has(tx.getAttributeType(resourceType6).create(7.5));

            entity2.has(tx.getAttributeType(resourceType5).create(-7L))
                    .has(tx.getAttributeType(resourceType6).create(7.5));

            entity3.has(tx.getAttributeType(resourceType1).create(1.8));

            entity4.has(tx.getAttributeType(resourceType2).create(0L))
                    .has(tx.getAttributeType(resourceType5).create(-7L))
                    .has(tx.getAttributeType(resourceType6).create(7.5));

            // some resources in, but not connect them to any instances
            aDisconnectedAttribute = tx.getAttributeType(resourceType1).create(2.8).id();
            tx.getAttributeType(resourceType2).create(-5L);
            tx.getAttributeType(resourceType3).create(100L);
            tx.getAttributeType(resourceType5).create(10L);
            tx.getAttributeType(resourceType6).create(0.8);

            tx.commit();
        }
    }
}