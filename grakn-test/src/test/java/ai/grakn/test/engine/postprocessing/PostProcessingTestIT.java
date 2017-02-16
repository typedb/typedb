/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Ltd
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.test.engine.postprocessing;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.concept.ResourceType;
import ai.grakn.engine.postprocessing.EngineCache;
import ai.grakn.engine.postprocessing.PostProcessing;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.exception.ConceptNotUniqueException;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.test.EngineContext;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class PostProcessingTestIT {
    private PostProcessing postProcessing = PostProcessing.getInstance();
    private EngineCache cache = EngineCache.getInstance();

    private GraknGraph graph;

    @ClassRule
    public static final EngineContext engine = EngineContext.startDistributedServer();

    @Before
    public void setUp() throws Exception {
        graph = engine.graphWithNewKeyspace();
        ((Logger) org.slf4j.LoggerFactory.getLogger(ConfigProperties.LOG_NAME_POSTPROCESSING_DEFAULT)).setLevel(Level.ALL);
    }

    @Test
    public void checkThatDuplicateResourcesAtLargerScale() throws GraknValidationException, ExecutionException, InterruptedException {
        int numAttempts = 500;
        ExecutorService pool = Executors.newFixedThreadPool(40);
        Set<Future> futures = new HashSet<>();

        //Create Simple Ontology
        ResourceType<String> res1 = graph.putResourceType("res1", ResourceType.DataType.STRING);
        ResourceType<String> res2 = graph.putResourceType("res2", ResourceType.DataType.STRING);
        graph.putEntityType("e1").hasResource(res1);
        graph.putEntityType("e1").hasResource(res2);

        graph.commit();

        //Try to force duplicate resources
        for(int i = 0; i < numAttempts; i++){
            futures.add(pool.submit(() -> forceDuplicateResources(graph, "res1", "1")));
            futures.add(pool.submit(() -> forceDuplicateResources(graph, "res1", "2")));
            futures.add(pool.submit(() -> forceDuplicateResources(graph, "res1", "3")));
            futures.add(pool.submit(() -> forceDuplicateResources(graph, "res1", "4")));
            futures.add(pool.submit(() -> forceDuplicateResources(graph, "res1", "5")));

            futures.add(pool.submit(() -> forceDuplicateResources(graph, "res2", "1")));
            futures.add(pool.submit(() -> forceDuplicateResources(graph, "res2", "2")));
            futures.add(pool.submit(() -> forceDuplicateResources(graph, "res2", "3")));
            futures.add(pool.submit(() -> forceDuplicateResources(graph, "res2", "4")));
            futures.add(pool.submit(() -> forceDuplicateResources(graph, "res2", "5")));
        }

        for (Future future : futures) {
            future.get();
        }

        //Give some time for jobs to go through REST API
        Thread.sleep(5000);

        //Wait for cache to have some jobs
        waitForCache(graph.getKeyspace(), 2);

        //Check current broken state of graph
        graph.close();
        graph = Grakn.factory(Grakn.DEFAULT_URI, graph.getKeyspace()).getGraph();

        boolean res1IsBroken = graph.getResourceType("res1").instances().size() >= 2;
        boolean res2IsBroken = graph.getResourceType("res2").instances().size() >= 2;

        assertTrue("Failed at breaking resource 1 or 2", res1IsBroken || res2IsBroken);

        //Force PP
        postProcessing.run();

        //Check current broken state of graph
        graph.close();
        graph = Grakn.factory(Grakn.DEFAULT_URI, graph.getKeyspace()).getGraph();

        res1IsBroken = graph.getResourceType("res1").instances().size() >= 2;
        res2IsBroken = graph.getResourceType("res2").instances().size() >= 2;

        assertFalse("Failed at fixing resource 1 or 2", res1IsBroken || res2IsBroken);
    }

    private void forceDuplicateResources(GraknGraph graph, String resourceType, String resourceValue){
        try {
            graph.open();
            graph.getResourceType(resourceType).putResource(resourceValue);
            graph.commit();
        } catch (GraknValidationException | ConceptNotUniqueException e) {
            //Ignore
        } finally {
            graph.close();
        }
    }

    private void waitForCache(String keyspace, int value) throws InterruptedException {
        boolean flag = true;
        while(flag){
            if(cache.getNumResourceJobs(keyspace) < value){
                Thread.sleep(1000);
            } else {
                flag = false;
            }
        }
    }
}