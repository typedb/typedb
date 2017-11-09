package ai.grakn.test.graql.analytics;

import ai.grakn.Grakn;
import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.graql.Graql;
import ai.grakn.test.EngineContext;
import ai.grakn.test.GraknTestSetup;
import ai.grakn.util.Schema;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.internal.analytics.Utility.getResourceEdgeId;
import static ai.grakn.test.GraknTestSetup.usingTinker;
import static ai.grakn.util.SampleKBLoader.randomKeyspace;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ShortestPathTest {
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
    private ConceptId relationId1A12;

    public GraknSession factory;

    @ClassRule
    public static final EngineContext context = usingTinker() ? null : EngineContext.createWithInMemoryRedis();

    @Before
    public void setUp() {
        factory = usingTinker() ? Grakn.session(Grakn.IN_MEMORY, randomKeyspace()) : context.sessionWithNewKeyspace();
    }

    @Test(expected = GraqlQueryException.class)
    public void testShortestPathExceptionIdNotFound() throws Exception {
        // test on an empty graph
        try (GraknTx graph = factory.open(GraknTxType.READ)) {
            graph.graql().compute().path().from(entityId1).to(entityId2).execute();
        }
    }

    @Test(expected = GraqlQueryException.class)
    public void testShortestPathExceptionIdNotFoundSubgraph() throws Exception {
        addSchemaAndEntities();
        try (GraknTx graph = factory.open(GraknTxType.READ)) {
            graph.graql().compute().path().from(entityId1).to(entityId4).in(thing, related).execute();
        }
    }

    @Test
    public void testShortestPathExceptionPathNotFound() throws Exception {
        addSchemaAndEntities();
        try (GraknTx graph = factory.open(GraknTxType.READ)) {
            assertFalse(graph.graql().compute().path().from(entityId1).to(entityId5).execute().isPresent());
        }
    }

    @Test
    public void testShortestPath() throws Exception {
        List<String> correctPath;
        List<String> computedPath;
        addSchemaAndEntities();

        try (GraknTx graph = factory.open(GraknTxType.READ)) {
            // directly connected vertices
            correctPath = Lists.newArrayList(entityId1.getValue(), relationId12.getValue());
            computedPath = graph.graql().compute().path().from(entityId1).to(relationId12).execute()
                    .get().stream().map(Concept::getId).map(ConceptId::getValue).collect(Collectors.toList());
            checkPath(correctPath, computedPath);

            Collections.reverse(correctPath);
            computedPath = Graql.compute().withTx(graph).path().to(entityId1).from(relationId12).execute()
                    .get().stream().map(Concept::getId).map(ConceptId::getValue).collect(Collectors.toList());
            checkPath(correctPath, computedPath);

            // entities connected by a relation
            correctPath = Lists.newArrayList(entityId1.getValue(), relationId12.getValue(), entityId2.getValue());
            computedPath = graph.graql().compute().path().from(entityId1).to(entityId2).execute()
                    .get().stream().map(Concept::getId).map(ConceptId::getValue).collect(Collectors.toList());
            checkPath(correctPath, computedPath);

            Collections.reverse(correctPath);
            computedPath = graph.graql().compute().path().to(entityId1).from(entityId2).execute()
                    .get().stream().map(Concept::getId).map(ConceptId::getValue).collect(Collectors.toList());
            checkPath(correctPath, computedPath);

            // only one path exists with given subtypes
            correctPath = Lists.newArrayList(entityId2.getValue(), relationId12.getValue(),
                    entityId1.getValue(), relationId13.getValue(), entityId3.getValue());
            computedPath = Graql.compute().withTx(graph).path().to(entityId3).from(entityId2).in(thing, related).execute()
                    .get().stream().map(Concept::getId).map(ConceptId::getValue).collect(Collectors.toList());
            checkPath(correctPath, computedPath);

            Collections.reverse(correctPath);
            computedPath = graph.graql().compute().path().in(thing, related).to(entityId2).from(entityId3).execute()
                    .get().stream().map(Concept::getId).map(ConceptId::getValue).collect(Collectors.toList());
            checkPath(correctPath, computedPath);

            correctPath = Lists.newArrayList(entityId1.getValue(), relationId12.getValue(), entityId2.getValue());
            computedPath = graph.graql().compute().path().in(thing, related).to(entityId2).from(entityId1).execute()
                    .get().stream().map(Concept::getId).map(ConceptId::getValue).collect(Collectors.toList());
            checkPath(correctPath, computedPath);

            Collections.reverse(correctPath);
            computedPath = graph.graql().compute().path().in(thing, related).from(entityId2).to(entityId1).execute()
                    .get().stream().map(Concept::getId).map(ConceptId::getValue).collect(Collectors.toList());
            checkPath(correctPath, computedPath);
        }
    }

    @Test
    public void testShortestPathConcurrency() {
        List<String> correctPath;
        addSchemaAndEntities();

        correctPath = Lists.newArrayList(entityId2.getValue(), relationId12.getValue(), entityId1.getValue());

        List<Long> list = new ArrayList<>(2);
        long workerNumber = 2L;
        if (GraknTestSetup.usingTinker()) workerNumber = 1L;
        for (long i = 0L; i < workerNumber; i++) {
            list.add(i);
        }
        Set<List<String>> result = list.parallelStream().map(i -> {
            try (GraknTx graph = factory.open(GraknTxType.READ)) {
                return graph.graql().compute().path().in(thing, related).from(entityId2).to(entityId1)
                        .execute().get().stream().map(concept -> concept.getId().getValue())
                        .collect(Collectors.toList());
            }
        }).collect(Collectors.toSet());
        result.forEach(path -> checkPath(correctPath, path));
    }

    @Test
    public void testShortestPathCastingWithThreeMessages() throws Exception {
        List<String> correctPath;
        List<String> computedPath;
        addSchemaAndEntities2();

        try (GraknTx graph = factory.open(GraknTxType.READ)) {
            correctPath = Lists.newArrayList(entityId2.getValue(), relationId12.getValue(),
                    entityId1.getValue(), relationId13.getValue(), entityId3.getValue());
            computedPath = graph.graql().compute().path().from(entityId2).to(entityId3).execute()
                    .get().stream().map(Concept::getId).map(ConceptId::getValue).collect(Collectors.toList());
            checkPath(correctPath, computedPath);

            Collections.reverse(correctPath);
            computedPath = graph.graql().compute().path().to(entityId2).from(entityId3).execute()
                    .get().stream().map(Concept::getId).map(ConceptId::getValue).collect(Collectors.toList());
            checkPath(correctPath, computedPath);

            correctPath = Lists.newArrayList(relationId1A12.getValue(), entityId1.getValue(),
                    relationId13.getValue(), entityId3.getValue());
            computedPath = graph.graql().compute().path().from(relationId1A12).to(entityId3).execute()
                    .get().stream().map(Concept::getId).map(ConceptId::getValue).collect(Collectors.toList());
            checkPath(correctPath, computedPath);

            Collections.reverse(correctPath);
            computedPath = graph.graql().compute().path().to(relationId1A12).from(entityId3).execute()
                    .get().stream().map(Concept::getId).map(ConceptId::getValue).collect(Collectors.toList());
            checkPath(correctPath, computedPath);
        }
    }

    @Test
    public void testMultipleIndependentShortestPaths() throws InvalidKBException {
        Set<List<ConceptId>> validPaths = new HashSet<>();
        ConceptId startId;
        ConceptId endId;

        try (GraknTx graph = factory.open(GraknTxType.WRITE)) {
            EntityType entityType = graph.putEntityType(thing);

            Role role1 = graph.putRole("role1");
            Role role2 = graph.putRole("role2");
            entityType.plays(role1).plays(role2);
            RelationshipType relationshipType = graph.putRelationshipType(related).relates(role1).relates(role2);

            Entity start = entityType.addEntity();
            Entity end = entityType.addEntity();

            startId = start.getId();
            endId = end.getId();

            // create N identical length paths
            int numberOfPaths = 10;

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

        try (GraknTx graph = factory.open(GraknTxType.READ)) {
            Optional<List<Concept>> result = graph.graql().compute().path().from(startId).to(endId).execute();
            assertEquals(1, validPaths.stream().filter(path -> checkPathsAreEqual(path, result)).count());
        }
    }

    @Test
    public void testResourceEdges() {
        ConceptId startId;
        ConceptId endId;
        try (GraknTx graph = factory.open(GraknTxType.WRITE)) {
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

        try (GraknTx graph = factory.open(GraknTxType.READ)) {
            Optional<List<Concept>> result = graph.graql().compute()
                    .path().from(startId).includeAttribute().to(endId).execute();
            assertEquals(3, result.get().size());
            assertEquals("@has-name", result.get().get(1).asRelationship().type().getLabel().getValue());
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

        try (GraknTx tx = factory.open(GraknTxType.WRITE)) {
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

        try (GraknTx graph = factory.open(GraknTxType.READ)) {
            Optional<List<Concept>> result;

            // Path from power3 to power3
            pathPerson3Power3.add(idPerson3);
            if (null != getResourceEdgeId(graph, idPower3, idPerson3)) {
                pathPerson3Power3.add(getResourceEdgeId(graph, idPower3, idPerson3));
            }
            pathPerson3Power3.add(idPower3);
            result = graph.graql().compute().path().from(idPerson3).to(idPower3).includeAttribute().execute();
            assertTrue(checkPathsAreEqual(pathPerson3Power3, result));

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

            result = graph.graql().compute().path().from(idPerson2).to(idPower1).includeAttribute().execute();
            assertTrue(checkPathsAreEqual(pathPerson2Power1, result));

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

            result = graph.graql().compute().path().includeAttribute().from(idPower3).to(idPower1).execute();
            assertTrue(checkPathsAreEqual(pathPower3Power1, result));
        }
    }

    private boolean checkPathsAreEqual(List<ConceptId> correctPath, Optional<List<Concept>> computedPath) {
        if (computedPath.isPresent()) {
            List<Concept> actualPath = computedPath.get();
            if (actualPath.isEmpty()) {
                return correctPath.isEmpty();
            } else {
                if (actualPath.size() != correctPath.size()) return false;
                ListIterator<Concept> elements = actualPath.listIterator();
                boolean returnState = true;
                while (elements.hasNext()) {
                    returnState &= (correctPath.get(elements.nextIndex()).equals(elements.next().getId()));
                }
                return returnState;
            }
        } else {
            return correctPath.isEmpty();
        }
    }

    private void checkPath(List<String> correctPath, List<String> computedPath) {
        assertEquals(correctPath.size(), computedPath.size());
        for (int i = 0; i < correctPath.size(); i++) {
            assertEquals(correctPath.get(i), computedPath.get(i));
        }
    }

    private void addSchemaAndEntities() throws InvalidKBException {
        try (GraknTx graph = factory.open(GraknTxType.WRITE)) {
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

    private void addSchemaAndEntities2() throws InvalidKBException {
        try (GraknTx graph = factory.open(GraknTxType.WRITE)) {
            EntityType entityType = graph.putEntityType(thing);

            Entity entity1 = entityType.addEntity();
            Entity entity2 = entityType.addEntity();
            Entity entity3 = entityType.addEntity();

            entityId1 = entity1.getId();
            entityId2 = entity2.getId();
            entityId3 = entity3.getId();

            Role role1 = graph.putRole("role1");
            Role role2 = graph.putRole("role2");
            entityType.plays(role1).plays(role2);
            RelationshipType relationshipType = graph.putRelationshipType(related).relates(role1).relates(role2);

            Role role3 = graph.putRole("role3");
            Role role4 = graph.putRole("role4");
            entityType.plays(role3).plays(role4);
            relationshipType.plays(role3).plays(role4);
            RelationshipType relationshipType2 = graph.putRelationshipType(veryRelated).relates(role3).relates(role4);

            relationId12 = relationshipType.addRelationship()
                    .addRolePlayer(role1, entity1)
                    .addRolePlayer(role2, entity2).getId();
            relationId13 = relationshipType.addRelationship()
                    .addRolePlayer(role1, entity1)
                    .addRolePlayer(role2, entity3).getId();

            relationId1A12 = relationshipType2.addRelationship()
                    .addRolePlayer(role3, entity1)
                    .addRolePlayer(role4, graph.getConcept(relationId12)).getId();

            graph.commit();
        }
    }
}
