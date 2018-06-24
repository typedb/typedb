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
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.graql.Graql;
import ai.grakn.test.rule.SessionContext;
import ai.grakn.util.GraknTestUtil;
import ai.grakn.util.Schema;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.internal.analytics.Utility.getResourceEdgeId;
import static ai.grakn.util.GraqlSyntax.Compute.Method.PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;

public class PathTest {
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

    public GraknSession session;

    @ClassRule
    public final static SessionContext sessionContext = SessionContext.create();

    @Before
    public void setUp() {
        session = sessionContext.newSession();
    }

    @Test(expected = GraqlQueryException.class)
    public void testShortestPathExceptionIdNotFound() {
        // test on an empty graph
        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            graph.graql().compute(PATH).from(entityId1).to(entityId2).execute();
        }
    }

    @Test(expected = GraqlQueryException.class)
    public void testShortestPathExceptionIdNotFoundSubgraph() {
        addSchemaAndEntities();
        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            graph.graql().compute(PATH).from(entityId1).to(entityId4).in(thing, related).execute();
        }
    }

    @Test
    public void whenThereIsNoPath_PathReturnsEmptyOptional() {
        addSchemaAndEntities();
        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            assertEquals(Collections.emptyList(), graph.graql().compute(PATH).from(entityId1).to(entityId5).execute().getPaths().get());
        }
    }

    @Test
    public void testShortestPath() {
        List<ConceptId> correctPath;
        List<List<ConceptId>> allPaths;
        addSchemaAndEntities();

        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            // directly connected vertices
            correctPath = Lists.newArrayList(entityId1, relationId12);
            List<ConceptId> path = graph.graql().compute(PATH).from(entityId1).to(relationId12).execute().getPaths().get().get(0);
            checkPath(correctPath, path);

            Collections.reverse(correctPath);
            allPaths = Graql.compute(PATH).withTx(graph).to(entityId1).from(relationId12).execute().getPaths().get();
            checkPath(correctPath, allPaths.get(0));

            // entities connected by a relation
            correctPath = Lists.newArrayList(entityId1, relationId12, entityId2);
            allPaths = graph.graql().compute(PATH).from(entityId1).to(entityId2).execute().getPaths().get();
            assertEquals(1, allPaths.size());
            checkPath(correctPath, allPaths.get(0));

            Collections.reverse(correctPath);
            allPaths = graph.graql().compute(PATH).to(entityId1).from(entityId2).execute().getPaths().get();
            assertEquals(1, allPaths.size());
            checkPath(correctPath, allPaths.get(0));

            // only one wpath exists with given subtypes
            correctPath = Lists.newArrayList(entityId2, relationId12, entityId1, relationId13, entityId3);
            allPaths = graph.graql().compute(PATH).to(entityId3).from(entityId2).in(thing, related).execute().getPaths().get();
            assertEquals(1, allPaths.size());
            checkPath(correctPath, allPaths.get(0));

            Collections.reverse(correctPath);
            allPaths = graph.graql().compute(PATH).in(thing, related).to(entityId2).from(entityId3).execute().getPaths().get();
            assertEquals(1, allPaths.size());
            checkPath(correctPath, allPaths.get(0));

            correctPath = Lists.newArrayList(entityId1, relationId12, entityId2);
            allPaths = graph.graql().compute(PATH).in(thing, related).to(entityId2).from(entityId1).execute().getPaths().get();
            assertEquals(1, allPaths.size());
            checkPath(correctPath, allPaths.get(0));

            Collections.reverse(correctPath);
            allPaths = graph.graql().compute(PATH).in(thing, related).from(entityId2).to(entityId1).execute().getPaths().get();
            assertEquals(1, allPaths.size());
            checkPath(correctPath, allPaths.get(0));
        }
    }

    @Test
    public void testShortestPathConcurrency() {
        assumeFalse(GraknTestUtil.usingTinker());

        List<ConceptId> correctPath;
        addSchemaAndEntities();

        correctPath = Lists.newArrayList(entityId2, relationId12, entityId1);

        List<Integer> list = new ArrayList<>();
        int workerNumber = 3;
        for (int i = 0; i < workerNumber; i++) {
            list.add(i);
        }
        List<List<List<ConceptId>>> result = list.parallelStream().map(i -> {
            try (GraknTx graph = session.transaction(GraknTxType.READ)) {
                return graph.graql().compute(PATH).in(thing, related).from(entityId2).to(entityId1).execute().getPaths().get();
            }
        }).collect(Collectors.toList());

        result.forEach(allPaths -> {
            assertEquals(1, allPaths.size());
            checkPath(correctPath, allPaths.get(0));
        });
    }

    @Test
    public void testMultipleIndependentShortestPaths() throws InvalidKBException {
        Set<List<ConceptId>> validPaths = new HashSet<>();
        ConceptId startId;
        ConceptId endId;

        int numberOfPaths = 3;
        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            EntityType entityType = graph.putEntityType(thing);

            Role role1 = graph.putRole("role1");
            Role role2 = graph.putRole("role2");
            entityType.plays(role1).plays(role2);
            RelationshipType relationshipType = graph.putRelationshipType(related).relates(role1).relates(role2);

            Entity start = entityType.addEntity();
            Entity end = entityType.addEntity();

            startId = start.getId();
            endId = end.getId();

            // create N identical length path
            for (int i = 0; i < numberOfPaths; i++) {
                List<ConceptId> validPath = new ArrayList<>();
                validPath.add(startId);

                Entity middle = entityType.addEntity();
                ConceptId middleId = middle.getId();
                ConceptId assertion1 = relationshipType.addRelationship()
                        .addRolePlayer(role1, start)
                        .addRolePlayer(role2, middle).getId();

                validPath.add(assertion1);
                validPath.add(middleId);

                ConceptId assertion2 = relationshipType.addRelationship()
                        .addRolePlayer(role1, middle)
                        .addRolePlayer(role2, end).getId();

                validPath.add(assertion2);
                validPath.add(endId);
                validPaths.add(validPath);
            }

            graph.commit();
        }

        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            List<List<ConceptId>> allPaths = graph.graql().compute(PATH).from(startId).to(endId).execute().getPaths().get();
            assertEquals(numberOfPaths, allPaths.size());
            Set<List<ConceptId>> allPathsSet = new HashSet<>(allPaths);
            assertEquals(validPaths, allPathsSet);
        }
    }

    @Test
    public void testMultiplePathsSharing1Instance() throws InvalidKBException {
        ConceptId startId;
        ConceptId endId;
        Set<List<ConceptId>> correctPaths = new HashSet<>();

        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            EntityType entityType = graph.putEntityType(thing);

            Role role1 = graph.putRole("role1");
            Role role2 = graph.putRole("role2");
            entityType.plays(role1).plays(role2);
            RelationshipType relationshipType1 = graph.putRelationshipType(related).relates(role1).relates(role2);

            Role role3 = graph.putRole("role3");
            Role role4 = graph.putRole("role4");
            entityType.plays(role3).plays(role4);
            RelationshipType relationshipType2 = graph.putRelationshipType(veryRelated).relates(role3).relates(role4);

            Entity start = entityType.addEntity();
            Entity end = entityType.addEntity();
            Entity middle = entityType.addEntity();

            startId = start.getId();
            endId = end.getId();
            ConceptId middleId = middle.getId();

            ConceptId assertion11 = relationshipType1.addRelationship()
                    .addRolePlayer(role1, start)
                    .addRolePlayer(role2, middle).getId();

            ConceptId assertion12 = relationshipType1.addRelationship()
                    .addRolePlayer(role1, middle)
                    .addRolePlayer(role2, end).getId();

            ConceptId assertion21 = relationshipType2.addRelationship()
                    .addRolePlayer(role3, start)
                    .addRolePlayer(role4, middle).getId();

            ConceptId assertion22 = relationshipType2.addRelationship()
                    .addRolePlayer(role3, middle)
                    .addRolePlayer(role4, end).getId();


            correctPaths.add(Lists.newArrayList(startId, assertion11, middleId, assertion12, endId));
            correctPaths.add(Lists.newArrayList(startId, assertion11, middleId, assertion22, endId));
            correctPaths.add(Lists.newArrayList(startId, assertion21, middleId, assertion12, endId));
            correctPaths.add(Lists.newArrayList(startId, assertion21, middleId, assertion22, endId));

            graph.commit();
        }

        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            List<List<ConceptId>> allPaths = graph.graql().compute(PATH).from(startId).to(endId).execute().getPaths().get();
            assertEquals(correctPaths.size(), allPaths.size());
            Set<List<ConceptId>> allPathsSet = new HashSet<>(allPaths);
            assertEquals(correctPaths, allPathsSet);
        }
    }

    @Test
    public void testMultiplePathsSharing3Instances() throws InvalidKBException {
        ConceptId startId;
        ConceptId endId;
        Set<List<ConceptId>> correctPaths = new HashSet<>();

        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            EntityType entityType = graph.putEntityType(thing);

            Role role1 = graph.putRole("role1");
            Role role2 = graph.putRole("role2");
            entityType.plays(role1).plays(role2);
            RelationshipType relationshipType1 = graph.putRelationshipType(related).relates(role1).relates(role2);

            Role role3 = graph.putRole("role3");
            Role role4 = graph.putRole("role4");
            Role role5 = graph.putRole("role5");
            entityType.plays(role3).plays(role4).plays(role5);
            RelationshipType relationshipType2 = graph.putRelationshipType(veryRelated)
                    .relates(role3).relates(role4).relates(role5);

            Entity start = entityType.addEntity();
            Entity end = entityType.addEntity();
            Entity middle = entityType.addEntity();
            Entity middleA = entityType.addEntity();
            Entity middleB = entityType.addEntity();

            startId = start.getId();
            endId = end.getId();
            ConceptId middleId = middle.getId();
            ConceptId middleAId = middleA.getId();
            ConceptId middleBId = middleB.getId();

            ConceptId assertion1 = relationshipType1.addRelationship()
                    .addRolePlayer(role1, start)
                    .addRolePlayer(role2, middle).getId();

            ConceptId assertion2 = relationshipType2.addRelationship()
                    .addRolePlayer(role3, middle)
                    .addRolePlayer(role4, middleA)
                    .addRolePlayer(role5, middleB).getId();

            ConceptId assertion1A = relationshipType1.addRelationship()
                    .addRolePlayer(role1, middleA)
                    .addRolePlayer(role2, end).getId();

            ConceptId assertion1B = relationshipType1.addRelationship()
                    .addRolePlayer(role1, middleB)
                    .addRolePlayer(role2, end).getId();

            List<ConceptId> sharedPath = Lists.newArrayList(startId, assertion1, middleId, assertion2);
            List<ConceptId> path1 = new ArrayList<>(sharedPath);
            path1.addAll(Lists.newArrayList(middleAId, assertion1A, endId));
            List<ConceptId> path2 = new ArrayList<>(sharedPath);
            path2.addAll(Lists.newArrayList(middleBId, assertion1B, endId));
            correctPaths.add(path1);
            correctPaths.add(path2);

            graph.commit();
        }

        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            List<List<ConceptId>> allPaths = graph.graql().compute(PATH).from(startId).to(endId).execute().getPaths().get();
            assertEquals(correctPaths.size(), allPaths.size());
            Set<List<ConceptId>> allPathsSet = new HashSet<>(allPaths);
            assertEquals(correctPaths, allPathsSet);
        }
    }

    @Test
    public void testMultiplePathInSubGraph() {
        Set<List<ConceptId>> correctPaths = new HashSet<>();
        List<List<ConceptId>> allPaths;
        addSchemaAndEntities();

        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            if (GraknTestUtil.usingJanus()) {
                // If no path is found in the vertex program, NoResultException is thrown to skip map reduce.
                // Tinker doesn't handle this well, so the next graql query would find the graph is empty.
                allPaths = graph.graql().compute(PATH).in(thing, anotherThing)
                        .to(entityId1).from(entityId4).execute().getPaths().get();
                assertEquals(0, allPaths.size());
            }

            correctPaths.add(Lists.newArrayList(entityId1, relationId12, entityId2, relationId24, entityId4));
            correctPaths.add(Lists.newArrayList(entityId1, relationId13, entityId3, relationId34, entityId4));
            allPaths = graph.graql().compute(PATH).from(entityId1).to(entityId4).execute().getPaths().get();
            assertEquals(correctPaths.size(), allPaths.size());
            Set<List<ConceptId>> computedPaths = new HashSet<>(allPaths);
            assertEquals(correctPaths, computedPaths);
        }
    }


    @Test
    public void testResourceEdges() {
        ConceptId startId;
        ConceptId endId;
        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            EntityType person = graph.putEntityType("person");
            AttributeType<String> name = graph.putAttributeType("name", AttributeType.DataType.STRING);
            person.attribute(name);
            Entity aPerson = person.addEntity();
            startId = aPerson.getId();
            Attribute<String> jason = name.putAttribute("jason");
            aPerson.attribute(jason);
            endId = jason.getId();

            graph.commit();
        }

        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            List<List<ConceptId>> allPaths = graph.graql().compute(PATH).from(startId).to(endId)
                    .includeAttributes(true).execute().getPaths().get();
            assertEquals(1, allPaths.size());
            assertEquals(3, allPaths.get(0).size());
            assertEquals("@has-name", graph.getConcept(allPaths.get(0).get(1))
                                                    .asRelationship()
                                                    .type()
                                                    .getLabel()
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

        try (GraknTx tx = session.transaction(GraknTxType.WRITE)) {
            EntityType person = tx.putEntityType("person");
            AttributeType<Long> power = tx.putAttributeType("power", AttributeType.DataType.LONG);

            person.attribute(power);

            // manually construct the attribute relation
            Role resourceOwner = tx.getRole(Schema.ImplicitType.HAS_OWNER.getLabel(Label.of("power")).getValue());
            Role resourceValue = tx.getRole(Schema.ImplicitType.HAS_VALUE.getLabel(Label.of("power")).getValue());
            RelationshipType relationType = tx.getRelationshipType(Schema.ImplicitType.HAS.getLabel(Label.of("power")).getValue());

            Entity person1 = person.addEntity();
            idPerson1 = person1.getId();
            Entity person2 = person.addEntity();
            idPerson2 = person2.getId();
            Entity person3 = person.addEntity();
            idPerson3 = person3.getId();

            Attribute power1 = power.putAttribute(1L);
            idPower1 = power1.getId();
            Attribute power2 = power.putAttribute(2L);
            idPower2 = power2.getId();
            Attribute power3 = power.putAttribute(3L);
            idPower3 = power3.getId();

            assert relationType != null;
            idRelationPerson1Power1 = relationType.addRelationship()
                    .addRolePlayer(resourceOwner, person1)
                    .addRolePlayer(resourceValue, power1).getId();

            idRelationPerson2Power2 = relationType.addRelationship()
                    .addRolePlayer(resourceOwner, person2)
                    .addRolePlayer(resourceValue, power2).getId();

            // add implicit resource relationships as well
            person.attribute(power);

            person1.attribute(power2);
            person3.attribute(power3);

            // finally add a relation between persons to make it more interesting
            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            person.plays(role1).plays(role2);
            RelationshipType relationTypePerson = tx.putRelationshipType(related).relates(role1).relates(role2);
            idRelationPerson1Person3 = relationTypePerson.addRelationship()
                    .addRolePlayer(role1, person1)
                    .addRolePlayer(role2, person3).getId();
            tx.commit();
        }

        try (GraknTx graph = session.transaction(GraknTxType.READ)) {
            List<List<ConceptId>> allPaths;

            // Path from power3 to power3
            pathPerson3Power3.add(idPerson3);
            if (null != getResourceEdgeId(graph, idPower3, idPerson3)) {
                pathPerson3Power3.add(getResourceEdgeId(graph, idPower3, idPerson3));
            }
            pathPerson3Power3.add(idPower3);
            allPaths = graph.graql().compute(PATH).from(idPerson3).to(idPower3).includeAttributes(true).execute().getPaths().get();
            assertEquals(1, allPaths.size());
            checkPathsAreEqual(pathPerson3Power3, allPaths.get(0));

            // Path from person2 to power1
            pathPerson2Power1.add(idPerson2);
            pathPerson2Power1.add(idRelationPerson2Power2);
            pathPerson2Power1.add(idPower2);
            if (null != getResourceEdgeId(graph, idPerson1, idPower2)) {
                pathPerson2Power1.add(getResourceEdgeId(graph, idPerson1, idPower2));
            }
            pathPerson2Power1.add(idPerson1);
            pathPerson2Power1.add(idRelationPerson1Power1);
            pathPerson2Power1.add(idPower1);

            allPaths = graph.graql().compute(PATH).from(idPerson2).to(idPower1).includeAttributes(true).execute().getPaths().get();
            assertEquals(1, allPaths.size());
            checkPathsAreEqual(pathPerson2Power1, allPaths.get(0));

            // Path from power3 to power1
            pathPower3Power1.add(idPower3);
            if (null != getResourceEdgeId(graph, idPower3, idPerson3)) {
                pathPower3Power1.add(getResourceEdgeId(graph, idPower3, idPerson3));
            }
            pathPower3Power1.add(idPerson3);
            pathPower3Power1.add(idRelationPerson1Person3);
            pathPower3Power1.add(idPerson1);
            pathPower3Power1.add(idRelationPerson1Power1);
            pathPower3Power1.add(idPower1);

            allPaths = graph.graql().compute(PATH).includeAttributes(true).from(idPower3).to(idPower1).execute().getPaths().get();
            assertEquals(1, allPaths.size());
            checkPathsAreEqual(pathPower3Power1, allPaths.get(0));
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
        try (GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            EntityType entityType1 = graph.putEntityType(thing);
            EntityType entityType2 = graph.putEntityType(anotherThing);

            Entity entity1 = entityType1.addEntity();
            Entity entity2 = entityType1.addEntity();
            Entity entity3 = entityType1.addEntity();
            Entity entity4 = entityType2.addEntity();
            Entity entity5 = entityType1.addEntity();

            entityId1 = entity1.getId();
            entityId2 = entity2.getId();
            entityId3 = entity3.getId();
            entityId4 = entity4.getId();
            entityId5 = entity5.getId();

            Role role1 = graph.putRole("role1");
            Role role2 = graph.putRole("role2");
            entityType1.plays(role1).plays(role2);
            entityType2.plays(role1).plays(role2);
            RelationshipType relationshipType = graph.putRelationshipType(related).relates(role1).relates(role2);

            relationId12 = relationshipType.addRelationship()
                    .addRolePlayer(role1, entity1)
                    .addRolePlayer(role2, entity2).getId();
            relationId13 = relationshipType.addRelationship()
                    .addRolePlayer(role1, entity1)
                    .addRolePlayer(role2, entity3).getId();
            relationId24 = relationshipType.addRelationship()
                    .addRolePlayer(role1, entity2)
                    .addRolePlayer(role2, entity4).getId();
            relationId34 = relationshipType.addRelationship()
                    .addRolePlayer(role1, entity3)
                    .addRolePlayer(role2, entity4).getId();

            graph.commit();
        }
    }
}
