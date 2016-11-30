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

package ai.grakn.graph.internal;

import ai.grakn.GraknGraph;
import ai.grakn.factory.OrientDBInternalFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class GraknOrientDBGraphTest {
    private static final String TEST_NAME = "grakntest";
    private static final String TEST_URI = "memory";
    private static final boolean TEST_BATCH_LOADING = false;
    private GraknGraph graknGraph;

    @Before
    public void setup(){
        graknGraph = new OrientDBInternalFactory(TEST_NAME, TEST_URI, null).getGraph(TEST_BATCH_LOADING);
    }

    @After
    public void cleanup(){
        graknGraph.clear();
    }

    @Ignore
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

    @Ignore
    @Test
    public void testRollback() {
        assertNull(graknGraph.getEntityType("X"));
        graknGraph.putEntityType("X");
        assertNotNull(graknGraph.getEntityType("X"));
        graknGraph.rollback();
        assertNull(graknGraph.getEntityType("X"));
    }

}