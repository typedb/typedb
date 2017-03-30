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

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
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

import static org.junit.Assert.assertEquals;

public class GraknOrientDBGraphTest {
    private static final String TEST_NAME = "grakntest";
    private final static String TEST_URI = Grakn.IN_MEMORY;
    private GraknGraph graknGraph;

    @Before
    public void setup(){
        graknGraph = new OrientDBInternalFactory(TEST_NAME, TEST_URI, null).open(GraknTxType.WRITE);
    }

    @After
    public void cleanup(){
        graknGraph.clear();
    }

    @Ignore // Failing remotely only
    @Test
    public void testTestThreadLocal(){
        ExecutorService pool = Executors.newFixedThreadPool(10);
        Set<Future> futures = new HashSet<>();
        graknGraph.putEntityType(UUID.randomUUID().toString());
        assertEquals(9, graknGraph.admin().getTinkerTraversal().toList().size());

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

        assertEquals(9, graknGraph.admin().getTinkerTraversal().toList().size());
    }
}