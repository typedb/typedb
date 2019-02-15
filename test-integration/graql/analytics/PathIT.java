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
import grakn.core.graql.concept.Attribute;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Entity;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.RelationType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.Schema;
import grakn.core.graql.query.Graql;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
import grakn.core.server.exception.InvalidKBException;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.core.graql.internal.analytics.Utility.getResourceEdgeId;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("CheckReturnValue")
public class PathIT {
    private static final String thing = "thingy";
    private static final String anotherThing = "anotherThing";
    private static final String related = "related";
    private static final String veryRelated = "veryRelated";

    private ConceptId entityId1;
    private ConceptId entityId2;
    private ConceptId entityId3;
    private ConceptId entityId4;
    private ConceptId entityId5;
    private ConceptId relationId12;
    private ConceptId relationId13;
    private ConceptId relationId24;
    private ConceptId relationId34;

    public Session session;

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    @Before
    public void setUp() {
        session = server.sessionWithNewKeyspace();
    }

    @After
    public void closeSession() { session.close(); }

    @Test(expected = GraqlQueryException.class)
    public void testShortestPathExceptionIdNotFound() {
        // test on an empty tx
        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            tx.execute(Graql.compute().path().from(entityId1).to(entityId2));
        }
    }

    @Test(expected = GraqlQueryException.class)
    public void testShortestPathExceptionIdNotFoundSubgraph() {
        addSchemaAndEntities();
        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            tx.execute(Graql.compute().path().from(entityId1).to(entityId4).in(thing, related));
        }
    }

    @Test
    public void whenThereIsNoPath_PathReturnsEmptyOptional() {
        addSchemaAndEntities();
        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            assertEquals(Collections.emptyList(), tx.execute(Graql.compute().path().from(entityId1).to(entityId5)));
        }
    }

    @Test
    public void testShortestPath() {
        List<ConceptId> correctPath;
        List<ConceptList> allPaths;
        addSchemaAndEntities();

        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            // directly connected vertices
            correctPath = Lists.newArrayList(entityId1, relationId12);
            ConceptList path = tx.execute(Graql.compute().path().from(entityId1).to(relationId12)).get(0);
            checkPath(correctPath, path.list());

            Collections.reverse(correctPath);
            allPaths = tx.execute(Graql.compute().path().to(entityId1).from(relationId12));
            checkPath(correctPath, allPaths.get(0).list());

            // entities connected by a relation
            correctPath = Lists.newArrayList(entityId1, relationId12, entityId2);
            allPaths = tx.execute(Graql.compute().path().from(entityId1).to(entityId2));
            assertEquals(1, allPaths.size());
            checkPath(correctPath, allPaths.get(0).list());

            Collections.reverse(correctPath);
            allPaths = tx.execute(Graql.compute().path().to(entityId1).from(entityId2));
            assertEquals(1, allPaths.size());
            checkPath(correctPath, allPaths.get(0).list());

            // only one wpath exists with given subtypes
            correctPath = Lists.newArrayList(entityId2, relationId12, entityId1, relationId13, entityId3);
            allPaths = tx.execute(Graql.compute().path().to(entityId3).from(entityId2).in(thing, related));
            assertEquals(1, allPaths.size());
            checkPath(correctPath, allPaths.get(0).list());

            Collections.reverse(correctPath);
            allPaths = tx.execute(Graql.compute().path().in(thing, related).to(entityId2).from(entityId3));
            assertEquals(1, allPaths.size());
            checkPath(correctPath, allPaths.get(0).list());

            correctPath = Lists.newArrayList(entityId1, relationId12, entityId2);
            allPaths = tx.execute(Graql.compute().path().in(thing, related).to(entityId2).from(entityId1));
            assertEquals(1, allPaths.size());
            checkPath(correctPath, allPaths.get(0).list());

            Collections.reverse(correctPath);
            allPaths = tx.execute(Graql.compute().path().in(thing, related).from(entityId2).to(entityId1));
            assertEquals(1, allPaths.size());
            checkPath(correctPath, allPaths.get(0).list());
        }
    }

    @Test
    public void testShortestPathConcurrency() {
        List<ConceptId> correctPath;
        addSchemaAndEntities();

        correctPath = Lists.newArrayList(entityId2, relationId12, entityId1);

        List<Integer> list = new ArrayList<>();
        int workerNumber = 3;
        for (int i = 0; i < workerNumber; i++) {
            list.add(i);
        }
        List<List<ConceptList>> result = list.parallelStream().map(i -> {
            try (Transaction tx = session.transaction(Transaction.Type.READ)) {
                return tx.execute(Graql.compute().path().in(thing, related).from(entityId2).to(entityId1));
            }
        }).collect(Collectors.toList());

        result.forEach(allPaths -> {
            assertEquals(1, allPaths.size());
            checkPath(correctPath, allPaths.get(0).list());
        });
    }

    @Test
    public void testMultipleIndependentShortestPaths() throws InvalidKBException {
        Set<List<ConceptId>> validPaths = new HashSet<>();
        ConceptId startId;
        ConceptId endId;

        int numberOfPaths = 3;
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            EntityType entityType = tx.putEntityType(thing);

            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            entityType.plays(role1).plays(role2);
            RelationType relationshipType = tx.putRelationshipType(related).relates(role1).relates(role2);

            Entity start = entityType.create();
            Entity end = entityType.create();

            startId = start.id();
            endId = end.id();

            // create N identical length path
            for (int i = 0; i < numberOfPaths; i++) {
                List<ConceptId> validPath = new ArrayList<>();
                validPath.add(startId);

                Entity middle = entityType.create();
                ConceptId middleId = middle.id();
                ConceptId assertion1 = relationshipType.create()
                        .assign(role1, start)
                        .assign(role2, middle).id();

                validPath.add(assertion1);
                validPath.add(middleId);

                ConceptId assertion2 = relationshipType.create()
                        .assign(role1, middle)
                        .assign(role2, end).id();

                validPath.add(assertion2);
                validPath.add(endId);
                validPaths.add(validPath);
            }

            tx.commit();
        }

        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            List<ConceptList> allPaths = tx.execute(Graql.compute().path().from(startId).to(endId));
            assertEquals(numberOfPaths, allPaths.size());

            Set<List<ConceptId>> allPathsSet = allPaths.stream().map(ConceptList::list).distinct().collect(Collectors.toSet());
            assertEquals(validPaths, allPathsSet);
        }
    }

    @Test
    public void testMultiplePathsSharing1Instance() throws InvalidKBException {
        ConceptId startId;
        ConceptId endId;
        Set<List<ConceptId>> correctPaths = new HashSet<>();

        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            EntityType entityType = tx.putEntityType(thing);

            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            entityType.plays(role1).plays(role2);
            RelationType relationshipType1 = tx.putRelationshipType(related).relates(role1).relates(role2);

            Role role3 = tx.putRole("role3");
            Role role4 = tx.putRole("role4");
            entityType.plays(role3).plays(role4);
            RelationType relationshipType2 = tx.putRelationshipType(veryRelated).relates(role3).relates(role4);

            Entity start = entityType.create();
            Entity end = entityType.create();
            Entity middle = entityType.create();

            startId = start.id();
            endId = end.id();
            ConceptId middleId = middle.id();

            ConceptId assertion11 = relationshipType1.create()
                    .assign(role1, start)
                    .assign(role2, middle).id();

            ConceptId assertion12 = relationshipType1.create()
                    .assign(role1, middle)
                    .assign(role2, end).id();

            ConceptId assertion21 = relationshipType2.create()
                    .assign(role3, start)
                    .assign(role4, middle).id();

            ConceptId assertion22 = relationshipType2.create()
                    .assign(role3, middle)
                    .assign(role4, end).id();


            correctPaths.add(Lists.newArrayList(startId, assertion11, middleId, assertion12, endId));
            correctPaths.add(Lists.newArrayList(startId, assertion11, middleId, assertion22, endId));
            correctPaths.add(Lists.newArrayList(startId, assertion21, middleId, assertion12, endId));
            correctPaths.add(Lists.newArrayList(startId, assertion21, middleId, assertion22, endId));

            tx.commit();
        }

        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            List<ConceptList> allPaths = tx.execute(Graql.compute().path().from(startId).to(endId));
            assertEquals(correctPaths.size(), allPaths.size());

            Set<List<ConceptId>> allPathsSet = allPaths.stream().map(ConceptList::list).collect(Collectors.toSet());
            assertEquals(correctPaths, allPathsSet);
        }
    }

    @Test
    public void testMultiplePathsSharing3Instances() throws InvalidKBException {
        ConceptId startId;
        ConceptId endId;
        Set<List<ConceptId>> correctPaths = new HashSet<>();

        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            EntityType entityType = tx.putEntityType(thing);

            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            entityType.plays(role1).plays(role2);
            RelationType relationshipType1 = tx.putRelationshipType(related).relates(role1).relates(role2);

            Role role3 = tx.putRole("role3");
            Role role4 = tx.putRole("role4");
            Role role5 = tx.putRole("role5");
            entityType.plays(role3).plays(role4).plays(role5);
            RelationType relationshipType2 = tx.putRelationshipType(veryRelated)
                    .relates(role3).relates(role4).relates(role5);

            Entity start = entityType.create();
            Entity end = entityType.create();
            Entity middle = entityType.create();
            Entity middleA = entityType.create();
            Entity middleB = entityType.create();

            startId = start.id();
            endId = end.id();
            ConceptId middleId = middle.id();
            ConceptId middleAId = middleA.id();
            ConceptId middleBId = middleB.id();

            ConceptId assertion1 = relationshipType1.create()
                    .assign(role1, start)
                    .assign(role2, middle).id();

            ConceptId assertion2 = relationshipType2.create()
                    .assign(role3, middle)
                    .assign(role4, middleA)
                    .assign(role5, middleB).id();

            ConceptId assertion1A = relationshipType1.create()
                    .assign(role1, middleA)
                    .assign(role2, end).id();

            ConceptId assertion1B = relationshipType1.create()
                    .assign(role1, middleB)
                    .assign(role2, end).id();

            List<ConceptId> sharedPath = Lists.newArrayList(startId, assertion1, middleId, assertion2);
            List<ConceptId> path1 = new ArrayList<>(sharedPath);
            path1.addAll(Lists.newArrayList(middleAId, assertion1A, endId));
            List<ConceptId> path2 = new ArrayList<>(sharedPath);
            path2.addAll(Lists.newArrayList(middleBId, assertion1B, endId));
            correctPaths.add(path1);
            correctPaths.add(path2);

            tx.commit();
        }

        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            List<ConceptList> allPaths = tx.execute(Graql.compute().path().from(startId).to(endId));
            assertEquals(correctPaths.size(), allPaths.size());
            Set<List<ConceptId>> allPathsSet = allPaths.stream().map(ConceptList::list).collect(Collectors.toSet());
            assertEquals(correctPaths, allPathsSet);
        }
    }

    @Test
    public void testMultiplePathInSubGraph() {
        Set<List<ConceptId>> correctPaths = new HashSet<>();
        List<ConceptList> allPaths;
        addSchemaAndEntities();

        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            allPaths = tx.execute(Graql.compute().path().in(thing, anotherThing)
                    .to(entityId1).from(entityId4));
            assertEquals(0, allPaths.size());

            correctPaths.add(Lists.newArrayList(entityId1, relationId12, entityId2, relationId24, entityId4));
            correctPaths.add(Lists.newArrayList(entityId1, relationId13, entityId3, relationId34, entityId4));
            allPaths = tx.execute(Graql.compute().path().from(entityId1).to(entityId4));
            assertEquals(correctPaths.size(), allPaths.size());

            Set<List<ConceptId>> computedPaths = allPaths.stream().map(ConceptList::list).collect(Collectors.toSet());
            assertEquals(correctPaths, computedPaths);
        }
    }


    @Test
    public void testResourceEdges() {
        ConceptId startId;
        ConceptId endId;
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            EntityType person = tx.putEntityType("person");
            AttributeType<String> name = tx.putAttributeType("name", AttributeType.DataType.STRING);
            person.has(name);
            Entity aPerson = person.create();
            startId = aPerson.id();
            Attribute<String> jason = name.create("jason");
            aPerson.has(jason);
            endId = jason.id();

            tx.commit();
        }

        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            List<ConceptList> allPaths = tx.execute(Graql.compute().path().from(startId).to(endId).attributes(true));
            assertEquals(1, allPaths.size());
            assertEquals(3, allPaths.get(0).list().size());
            assertEquals("@has-name", tx.getConcept(allPaths.get(0).list().get(1))
                    .asRelation()
                    .type()
                    .label()
                    .getValue());
        }
    }

    @Test
    public void testResourceVerticesAndEdges() {
        ConceptId idPerson1;
        ConceptId idPerson2;
        ConceptId idPerson3;

        ConceptId idPower1;
        ConceptId idPower2;
        ConceptId idPower3;

        ConceptId idRelationPerson1Power1;
        ConceptId idRelationPerson2Power2;

        ConceptId idRelationPerson1Person3;

        List<ConceptId> pathPerson2Power1 = new ArrayList<>();
        List<ConceptId> pathPower3Power1 = new ArrayList<>();
        List<ConceptId> pathPerson3Power3 = new ArrayList<>();

        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            EntityType person = tx.putEntityType("person");
            AttributeType<Long> power = tx.putAttributeType("power", AttributeType.DataType.LONG);

            person.has(power);

            // manually construct the attribute relation
            Role resourceOwner = tx.getRole(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of("power")).getValue());
            Role resourceValue = tx.getRole(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of("power")).getValue());
            RelationType relationType = tx.getRelationshipType(Schema.ImplicitType.HAS.getLabel(Label.of("power")).getValue());

            Entity person1 = person.create();
            idPerson1 = person1.id();
            Entity person2 = person.create();
            idPerson2 = person2.id();
            Entity person3 = person.create();
            idPerson3 = person3.id();

            Attribute power1 = power.create(1L);
            idPower1 = power1.id();
            Attribute power2 = power.create(2L);
            idPower2 = power2.id();
            Attribute power3 = power.create(3L);
            idPower3 = power3.id();

            assert relationType != null;
            idRelationPerson1Power1 = relationType.create()
                    .assign(resourceOwner, person1)
                    .assign(resourceValue, power1).id();

            idRelationPerson2Power2 = relationType.create()
                    .assign(resourceOwner, person2)
                    .assign(resourceValue, power2).id();

            // add implicit resource relationships as well
            person.has(power);

            person1.has(power2);
            person3.has(power3);

            // finally add a relation between persons to make it more interesting
            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            person.plays(role1).plays(role2);
            RelationType relationTypePerson = tx.putRelationshipType(related).relates(role1).relates(role2);
            idRelationPerson1Person3 = relationTypePerson.create()
                    .assign(role1, person1)
                    .assign(role2, person3).id();
            tx.commit();
        }

        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            List<ConceptList> allPaths;

            // Path from power3 to power3
            pathPerson3Power3.add(idPerson3);
            if (null != getResourceEdgeId(tx, idPower3, idPerson3)) {
                pathPerson3Power3.add(getResourceEdgeId(tx, idPower3, idPerson3));
            }
            pathPerson3Power3.add(idPower3);
            allPaths = tx.execute(Graql.compute().path().from(idPerson3).to(idPower3).attributes(true));
            assertEquals(1, allPaths.size());
            checkPathsAreEqual(pathPerson3Power3, allPaths.get(0).list());

            // Path from person2 to power1
            pathPerson2Power1.add(idPerson2);
            pathPerson2Power1.add(idRelationPerson2Power2);
            pathPerson2Power1.add(idPower2);
            if (null != getResourceEdgeId(tx, idPerson1, idPower2)) {
                pathPerson2Power1.add(getResourceEdgeId(tx, idPerson1, idPower2));
            }
            pathPerson2Power1.add(idPerson1);
            pathPerson2Power1.add(idRelationPerson1Power1);
            pathPerson2Power1.add(idPower1);

            allPaths = tx.execute(Graql.compute().path().from(idPerson2).to(idPower1).attributes(true));
            assertEquals(1, allPaths.size());
            checkPathsAreEqual(pathPerson2Power1, allPaths.get(0).list());

            // Path from power3 to power1
            pathPower3Power1.add(idPower3);
            if (null != getResourceEdgeId(tx, idPower3, idPerson3)) {
                pathPower3Power1.add(getResourceEdgeId(tx, idPower3, idPerson3));
            }
            pathPower3Power1.add(idPerson3);
            pathPower3Power1.add(idRelationPerson1Person3);
            pathPower3Power1.add(idPerson1);
            pathPower3Power1.add(idRelationPerson1Power1);
            pathPower3Power1.add(idPower1);

            allPaths = tx.execute(Graql.compute().path().attributes(true).from(idPower3).to(idPower1));
            assertEquals(1, allPaths.size());
            checkPathsAreEqual(pathPower3Power1, allPaths.get(0).list());
        }
    }

    private void checkPathsAreEqual(List<ConceptId> correctPath, List<ConceptId> computedPath) {
        assertEquals(correctPath.size(), computedPath.size());
        for (int i = 0; i < correctPath.size(); i++) {
            assertEquals(correctPath.get(i), computedPath.get(i));
        }
    }

    private void checkPath(List<ConceptId> correctPath, List<ConceptId> computedPath) {
        assertEquals(correctPath.size(), computedPath.size());
        for (int i = 0; i < correctPath.size(); i++) {
            assertEquals(correctPath.get(i), computedPath.get(i));
        }
    }

    private void addSchemaAndEntities() throws InvalidKBException {
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            EntityType entityType1 = tx.putEntityType(thing);
            EntityType entityType2 = tx.putEntityType(anotherThing);

            Entity entity1 = entityType1.create();
            Entity entity2 = entityType1.create();
            Entity entity3 = entityType1.create();
            Entity entity4 = entityType2.create();
            Entity entity5 = entityType1.create();

            entityId1 = entity1.id();
            entityId2 = entity2.id();
            entityId3 = entity3.id();
            entityId4 = entity4.id();
            entityId5 = entity5.id();

            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            entityType1.plays(role1).plays(role2);
            entityType2.plays(role1).plays(role2);
            RelationType relationshipType = tx.putRelationshipType(related).relates(role1).relates(role2);

            relationId12 = relationshipType.create()
                    .assign(role1, entity1)
                    .assign(role2, entity2).id();
            relationId13 = relationshipType.create()
                    .assign(role1, entity1)
                    .assign(role2, entity3).id();
            relationId24 = relationshipType.create()
                    .assign(role1, entity2)
                    .assign(role2, entity4).id();
            relationId34 = relationshipType.create()
                    .assign(role1, entity3)
                    .assign(role2, entity4).id();

            tx.commit();
        }
    }
}