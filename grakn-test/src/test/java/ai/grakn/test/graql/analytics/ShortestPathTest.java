package ai.grakn.test.graql.analytics;

import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Role;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.exception.InvalidGraphException;
import ai.grakn.graql.Graql;
import ai.grakn.test.EngineContext;
import ai.grakn.test.GraknTestSetup;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

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
    public static final EngineContext context = EngineContext.startInMemoryServer();

    @Before
    public void setUp() {
        factory = context.factoryWithNewKeyspace();
    }

    @Test(expected = GraqlQueryException.class)
    public void testShortestPathExceptionIdNotFound() throws Exception {
        // test on an empty graph
        try (GraknGraph graph = factory.open(GraknTxType.READ)) {
            graph.graql().compute().path().from(entityId1).to(entityId2).execute();
        }
    }

    @Test(expected = GraqlQueryException.class)
    public void testShortestPathExceptionIdNotFoundSubgraph() throws Exception {
        addOntologyAndEntities();
        try (GraknGraph graph = factory.open(GraknTxType.READ)) {
            graph.graql().compute().path().from(entityId1).to(entityId4).in(thing, related).execute();
        }
    }

    @Test
    public void testShortestPathExceptionPathNotFound() throws Exception {
        addOntologyAndEntities();
        try (GraknGraph graph = factory.open(GraknTxType.READ)) {
            assertFalse(graph.graql().compute().path().from(entityId1).to(entityId5).execute().isPresent());
        }
    }

    @Test
    public void testShortestPath() throws Exception {
        List<String> correctPath;
        List<String> computedPath;
        addOntologyAndEntities();

        try (GraknGraph graph = factory.open(GraknTxType.READ)) {
            // directly connected vertices
            correctPath = Lists.newArrayList(entityId1.getValue(), relationId12.getValue());
            computedPath = graph.graql().compute().path().from(entityId1).to(relationId12).execute()
                    .get().stream().map(Concept::getId).map(ConceptId::getValue).collect(Collectors.toList());
            checkPath(correctPath, computedPath);

            Collections.reverse(correctPath);
            computedPath = Graql.compute().withGraph(graph).path().to(entityId1).from(relationId12).execute()
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
            computedPath = Graql.compute().withGraph(graph).path().to(entityId3).from(entityId2).in(thing, related).execute()
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
        addOntologyAndEntities();
//        GraknSparkComputer.clear();

        correctPath = Lists.newArrayList(entityId2.getValue(), relationId12.getValue(), entityId1.getValue());

        List<Long> list = new ArrayList<>(2);
        long workerNumber = 2L;
        if (GraknTestSetup.usingTinker()) workerNumber = 1L;
        for (long i = 0L; i < workerNumber; i++) {
            list.add(i);
        }
        Set<List<String>> result = list.parallelStream().map(i -> {
            try (GraknGraph graph = factory.open(GraknTxType.READ)) {
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
        addOntologyAndEntities2();

        try (GraknGraph graph = factory.open(GraknTxType.READ)) {
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
    public void testMultipleIndependentShortestPaths() throws InvalidGraphException {
        Set<List<ConceptId>> validPaths = new HashSet<>();
        ConceptId startId;
        ConceptId endId;

        try (GraknGraph graph = factory.open(GraknTxType.WRITE)) {
            EntityType entityType = graph.putEntityType(thing);

            Role role1 = graph.putRole("role1");
            Role role2 = graph.putRole("role2");
            entityType.plays(role1).plays(role2);
            RelationType relationType = graph.putRelationType(related).relates(role1).relates(role2);

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
                ConceptId assertion1 = relationType.addRelation()
                        .addRolePlayer(role1, start)
                        .addRolePlayer(role2, middle).getId();

                validPath.add(assertion1);
                validPath.add(middleId);

                ConceptId assertion2 = relationType.addRelation()
                        .addRolePlayer(role1, middle)
                        .addRolePlayer(role2, end).getId();

                validPath.add(assertion2);
                validPath.add(endId);
                validPaths.add(validPath);
            }

            graph.commit();
        }

        try (GraknGraph graph = factory.open(GraknTxType.READ)) {
            Optional<List<Concept>> result = graph.graql().compute().path().from(startId).to(endId).execute();
            assertEquals(1, validPaths.stream().filter(path -> checkPathsAreEqual(path, result)).count());
        }
    }

    private boolean checkPathsAreEqual(List<ConceptId> correctPath, Optional<List<Concept>> computedPath) {
        if (computedPath.isPresent()) {
            List<Concept> actualPath = computedPath.get();
            if (actualPath.isEmpty()) {
                return correctPath.isEmpty();
            } else {
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

    private void addOntologyAndEntities() throws InvalidGraphException {
        try (GraknGraph graph = factory.open(GraknTxType.WRITE)) {
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
            RelationType relationType = graph.putRelationType(related).relates(role1).relates(role2);

            relationId12 = relationType.addRelation()
                    .addRolePlayer(role1, entity1)
                    .addRolePlayer(role2, entity2).getId();
            relationId13 = relationType.addRelation()
                    .addRolePlayer(role1, entity1)
                    .addRolePlayer(role2, entity3).getId();
            relationId24 = relationType.addRelation()
                    .addRolePlayer(role1, entity2)
                    .addRolePlayer(role2, entity4).getId();
            relationId34 = relationType.addRelation()
                    .addRolePlayer(role1, entity3)
                    .addRolePlayer(role2, entity4).getId();

            graph.commit();
        }
    }

    private void addOntologyAndEntities2() throws InvalidGraphException {
        try (GraknGraph graph = factory.open(GraknTxType.WRITE)) {
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
            RelationType relationType = graph.putRelationType(related).relates(role1).relates(role2);

            Role role3 = graph.putRole("role3");
            Role role4 = graph.putRole("role4");
            entityType.plays(role3).plays(role4);
            relationType.plays(role3).plays(role4);
            RelationType relationType2 = graph.putRelationType(veryRelated).relates(role3).relates(role4);

            relationId12 = relationType.addRelation()
                    .addRolePlayer(role1, entity1)
                    .addRolePlayer(role2, entity2).getId();
            relationId13 = relationType.addRelation()
                    .addRolePlayer(role1, entity1)
                    .addRolePlayer(role2, entity3).getId();

            relationId1A12 = relationType2.addRelation()
                    .addRolePlayer(role3, entity1)
                    .addRolePlayer(role4, graph.getConcept(relationId12)).getId();

            graph.commit();
        }
    }
}
