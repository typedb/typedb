/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.core;

import io.mindmaps.Mindmaps;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Entity;
import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.exception.MindmapsValidationException;
import org.javatuples.Pair;
import org.junit.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.mindmaps.IntegrationUtils.graphWithNewKeyspace;
import static io.mindmaps.IntegrationUtils.startTestEngine;

/**
 *
 */
public class ConcurrencyTest {

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
        graph.close();
    }

    @Ignore
    @Test
    public void testSameGraphMultipleThreads() throws InterruptedException {
        int numberOfThread = 3;
        graph.close();
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThread);
        for (int i=0;i<numberOfThread;i++) {
            executor.submit(() -> {
                try {
                    createGraph();
                } catch (MindmapsValidationException e) {
                    e.printStackTrace();
                }
            });
        }
        executor.awaitTermination(5000, TimeUnit.MILLISECONDS);

        MindmapsGraph graph = Mindmaps.factory(Mindmaps.DEFAULT_URI).getGraph(keyspace);

        graph.getRoleType("related1");
        graph.getRoleType("related2");
        graph.getEntityType("thing");
        graph.getRelationType("relationship");
        graph.getEntity("e1");
        graph.getEntity("e2");
        graph.getRelation("relation1");
    }

    public void createGraph() throws MindmapsValidationException {
        MindmapsGraph graph = Mindmaps.factory(Mindmaps.DEFAULT_URI).getGraph(keyspace);
        RoleType related1 = graph.putRoleType("related1");
        RoleType related2 = graph.putRoleType("related2");

        EntityType thing = graph.putEntityType("thing").playsRole(related1).playsRole(related2);
        RelationType relationship = graph.putRelationType("relationship").hasRole(related1).hasRole(related2);

        Entity e1 = graph.putEntity("e1", thing);
        Entity e2 = graph.putEntity("e2", thing);
        graph.putRelation("relation1",relationship).putRolePlayer(related1,e1).putRolePlayer(related2,e2);

        graph.commit();
    }
}
