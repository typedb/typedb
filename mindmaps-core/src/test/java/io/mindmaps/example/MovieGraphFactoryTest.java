package io.mindmaps.example;

import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.model.*;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class MovieGraphFactoryTest {

    private static Graph graph;
    private static MindmapsTransaction dao;

    @BeforeClass
    public static void setUp() throws IOException {
        MindmapsGraph mindmapsGraph = MindmapsTestGraphFactory.newEmptyGraph();
        MovieGraphFactory.loadGraph(mindmapsGraph);
        dao = mindmapsGraph.newTransaction();
        graph = mindmapsGraph.getGraph();
    }

    @Test
    public void testGraphExists() {
        assertNotNull(graph);
    }

    @Test
    public void testGraphHasConcepts() {
        assertTrue(graph.traversal().V().hasNext());
    }

    @Test
    public void testGraphHasEdges() {
        assertTrue(graph.traversal().E().hasNext());
    }

    @Test
    public void testGraphHasMovie() {
        EntityType movie = dao.getEntityType("movie");
        assertTrue(movie.superType().equals(dao.getEntityType("production")));
    }

    @Test
    public void testGraphHasTvShow() {
        EntityType tvShow = dao.getEntityType("tv-show");
        assertTrue(tvShow.superType().equals(dao.getEntityType("production")));
    }

    @Test
    public void testGraphHasGodfather() {
        Collection<Entity> concepts = dao.getEntitiesByValue("Godfather");
        assertEquals(1, concepts.size());
        assertEquals("Godfather", concepts.iterator().next().getId());
    }

    @Test
    public void testGodfatherHasResource() {
        ResourceType tmdbVoteCount = dao.getResourceType("tmdb-vote-count");
        Entity godfather = dao.getEntity("Godfather");
        Stream<Resource<?>> resources = godfather.resources().stream();
        assertTrue(resources.anyMatch(r -> r.type().equals(tmdbVoteCount) && r.getValue().equals(1000L)));
    }

    @Test
    public void testRulesExists() {
        RuleType ruleType = dao.getRuleType("a-rule-type");
        assertEquals(2, ruleType.instances().size());
    }
}