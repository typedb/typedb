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

package ai.grakn.test.engine.loader;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.ResourceType;
import ai.grakn.engine.backgroundtasks.distributed.ClusterManager;
import ai.grakn.engine.backgroundtasks.distributed.Scheduler;
import ai.grakn.engine.backgroundtasks.distributed.TaskRunner;
import ai.grakn.engine.backgroundtasks.taskstorage.GraknStateStorage;
import ai.grakn.engine.loader.Loader;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.test.EngineContext;
import ai.grakn.util.ErrorMessage;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import static ai.grakn.graql.Graql.match;
import static ai.grakn.graql.Graql.var;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;

public class LoaderTest {

    private Loader loader;
    private GraknGraph graph;

    @ClassRule
    public static final EngineContext engine = EngineContext.startServer();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @BeforeClass
    public static void startup() throws Exception {
        ((Logger) org.slf4j.LoggerFactory.getLogger(Loader.class)).setLevel(Level.DEBUG);
        ((Logger) org.slf4j.LoggerFactory.getLogger(GraknStateStorage.class)).setLevel(Level.DEBUG);
        ((Logger) org.slf4j.LoggerFactory.getLogger(Loader.class)).setLevel(Level.DEBUG);
        ((Logger) org.slf4j.LoggerFactory.getLogger(Scheduler.class)).setLevel(Level.DEBUG);
        ((Logger) org.slf4j.LoggerFactory.getLogger(TaskRunner.class)).setLevel(Level.DEBUG);
        ((Logger) org.slf4j.LoggerFactory.getLogger(ClusterManager.class)).setLevel(Level.DEBUG);
    }

    @Before
    public void setup() {
        //TODO fix this
        graph = engine.graphWithNewKeyspace();
        loader = new Loader(engine.getClusterManager(), graph.getKeyspace());
        loadOntology(graph.getKeyspace());
    }

    @Test
    public void loaderDefaultBatchSizeTest() {
        loadAndTime(60000);
    }

    @Test
    public void loaderNewBatchSizeTest() {
        loader.setBatchSize(20);
        loadAndTime(60000);
    }

    @Test
    public void loadWithSmallQueueSizeToBlockTest(){
        loader.setQueueSize(1);
        loadAndTime(60000);
    }

    @Test
    public void whenLoadingNormalDataThenDontTimeout(){
        // test the the total duration is longer than the timeout
        // however, the individual tasks will be completing faster than the timeout
        int timeout = 100;
        long startTime = System.currentTimeMillis();
        loadAndTime(timeout);
        long endTime = System.currentTimeMillis();
        assertThat(endTime-startTime, greaterThan(Integer.toUnsignedLong(timeout)));
    }

    @Test
    public void whenLoadingDataExceedsTimeoutThenTimeout(){
        exception.expectMessage(ErrorMessage.LOADER_WAIT_TIMEOUT.getMessage());
        int timeout = 1;
        loadAndTime(timeout);
    }

    public static void loadOntology(String keyspace){
        GraknGraph graph = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph();

        EntityType nameTag = graph.putEntityType("name_tag");
        ResourceType<String> nameTagString = graph.putResourceType("name_tag_string", ResourceType.DataType.STRING);
        ResourceType<String> nameTagId = graph.putResourceType("name_tag_id", ResourceType.DataType.STRING);

        nameTag.hasResource(nameTagString);
        nameTag.hasResource(nameTagId);

        try {
            graph.commit();
        } catch (GraknValidationException e){
            throw new RuntimeException(e);
        }
    }

    private long loadAndTime(int timeout){
        long startTime = System.currentTimeMillis();

        Collection<String> ids = new ArrayList<>();

        for(int i = 0; i < 50; i++){
            String id = UUID.randomUUID().toString();

            ids.add(id);

            InsertQuery query = Graql.insert(
                    var().isa("name_tag")
                            .has("name_tag_string", UUID.randomUUID().toString())
                            .has("name_tag_id", id));

            loader.add(query);
        }

        loader.waitToFinish(timeout);

        System.out.println("Time to load:");
        long duration = System.currentTimeMillis() - startTime;
        System.out.println(duration);

        Collection<Entity> nameTags = graph.getEntityType("name_tag").instances();

        assertEquals(50, nameTags.size());
        ids.stream().map(graph::getResourcesByValue).forEach(Assert::assertNotNull);

        return duration;
    }
}
