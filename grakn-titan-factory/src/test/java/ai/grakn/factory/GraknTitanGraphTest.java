/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
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

package ai.grakn.factory;

import ai.grakn.GraknGraph;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.exception.GraphRuntimeException;
import ai.grakn.graph.internal.GraknTitanGraph;
import ai.grakn.util.ErrorMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class GraknTitanGraphTest {
    private static final String TEST_CONFIG = "conf/grakn-titan-test.properties";
    private static final String TEST_NAME = "grakntest";
    private static final String TEST_URI = null;
    private static final boolean TEST_BATCH_LOADING = false;
    private GraknGraph graknGraph;

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup(){
        graknGraph = new TitanInternalFactory(TEST_NAME, TEST_URI, TEST_CONFIG).getGraph(TEST_BATCH_LOADING);
    }

    @After
    public void cleanup(){
        graknGraph.clear();
    }

    @Test
    public void testMultithreading(){
        Set<Future> futures = new HashSet<>();
        ExecutorService pool = Executors.newFixedThreadPool(10);

        for(int i = 0; i < 100; i ++){
            futures.add(pool.submit(() -> addEntityType(graknGraph)));
        }

        futures.forEach(future -> {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });

        assertEquals(108, graknGraph.getTinkerTraversal().toList().size());
    }
    private void addEntityType(GraknGraph graknGraph){
        graknGraph.putEntityType(UUID.randomUUID().toString());
        try {
            graknGraph.commit();
        } catch (GraknValidationException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testTestThreadLocal(){
        ExecutorService pool = Executors.newFixedThreadPool(10);
        Set<Future> futures = new HashSet<>();
        graknGraph.putEntityType(UUID.randomUUID().toString());
        assertEquals(9, graknGraph.getTinkerTraversal().toList().size());

        for(int i = 0; i < 100; i ++){
            futures.add(pool.submit(() -> {
                GraknGraph innerTranscation = this.graknGraph;
                innerTranscation.putEntityType(UUID.randomUUID().toString());
            }));
        }

        futures.forEach(future -> {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException ignored) {

            }
        });

        assertEquals(9, graknGraph.getTinkerTraversal().toList().size());
    }

    @Test
    public void testRollback() {
        assertNull(graknGraph.getEntityType("X"));
        graknGraph.putEntityType("X");
        assertNotNull(graknGraph.getEntityType("X"));
        graknGraph.rollback();
        assertNull(graknGraph.getEntityType("X"));
    }

    @Test
    public void testCaseSensitiveKeyspaces(){
        TitanInternalFactory factory1 = new TitanInternalFactory("case", TEST_URI, TEST_CONFIG);
        TitanInternalFactory factory2 = new TitanInternalFactory("Case", TEST_URI, TEST_CONFIG);
        GraknTitanGraph case1 = factory1.getGraph(TEST_BATCH_LOADING);
        GraknTitanGraph case2 = factory2.getGraph(TEST_BATCH_LOADING);

        assertEquals(case1.getKeyspace(), case2.getKeyspace());
    }

    @Test
    public void testClearTitanGraph(){
        GraknTitanGraph graph = new TitanInternalFactory("case", TEST_URI, TEST_CONFIG).getGraph(false);
        graph.clear();
        expectedException.expect(GraphRuntimeException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.CLOSED_CLEAR.getMessage())
        ));
        graph.getEntityType("thing");
    }
}