package io.mindmaps.graql.internal.analytics;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.EntityType;
import io.mindmaps.factory.MindmapsClient;
import org.javatuples.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.mindmaps.IntegrationUtils.graphWithNewKeyspace;
import static io.mindmaps.IntegrationUtils.startTestEngine;

public class Statistics {

    String keyspace;
    MindmapsGraph graph;

    long startTime;

    @BeforeClass
    public static void startController() throws Exception {
        startTestEngine();
    }

    @Before
    public void setUp() throws InterruptedException {
        Pair<MindmapsGraph, String> result = graphWithNewKeyspace();
        graph = result.getValue0();
        keyspace = result.getValue1();
    }

    @After
    public void cleanGraph() {
        graph.clear();
    }

    @Test
    public void testMax() throws Exception {
        graph = MindmapsClient.getGraph(keyspace);
        EntityType thing = graph.putEntityType("thing");
        EntityType anotherThing = graph.putEntityType("another");
        graph.putEntity("1", thing);
        graph.putEntity("2", thing);
        graph.putEntity("3", anotherThing);
        graph.commit();
        graph.close();

        Analytics computer = new Analytics(keyspace);

    }
}
