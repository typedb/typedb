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
import ai.grakn.engine.backgroundtasks.TaskManager;
import ai.grakn.engine.backgroundtasks.TaskState;
import ai.grakn.engine.TaskStatus;
import ai.grakn.engine.backgroundtasks.standalone.StandaloneTaskManager;
import ai.grakn.engine.backgroundtasks.taskstatestorage.TaskStateGraphStore;
import ai.grakn.engine.loader.LoaderClient;
import ai.grakn.engine.loader.LoaderTask;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.test.EngineContext;
import ai.grakn.util.ErrorMessage;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static ai.grakn.graql.Graql.var;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LoaderClientTest {

    private LoaderClient loader;
    private GraknGraph graph;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static final EngineContext engine = EngineContext.startDistributedServer();

    @Before
    public void setup() {
        graph = engine.graphWithNewKeyspace();
        loader = new LoaderClient(graph.getKeyspace(), Grakn.DEFAULT_URI);
        loadOntology(graph.getKeyspace());
    }

    @Test
    public void loaderDefaultBatchSizeTest() {
        loadAndTime();
    }

    @Test
    public void loaderNewBatchSizeTest() {
        loader.setBatchSize(20);
        loadAndTime();
    }

    @Test
    public void loadWithSmallQueueSizeToBlockTest(){
        loader.setQueueSize(1);
        loadAndTime();
    }

    @Test
    public void whenLoadingNormalDataThenDontTimeout(){
        // test the the total duration is longer than the timeout
        // however, the individual tasks will be completing faster than the timeout
        int timeout = 100;

        LoaderClient loaderWithFakeTaskManager = getFakeNormalLoader();

        long startTime = System.currentTimeMillis();
        loaderWithFakeTaskManager.waitToFinish();
        long endTime = System.currentTimeMillis();
        assertThat(endTime-startTime, greaterThan(Integer.toUnsignedLong(timeout)));
    }

    @Test
    public void whenLoadingDataExceedsTimeoutThenTimeout(){
        exception.expectMessage(ErrorMessage.LOADER_WAIT_TIMEOUT.getMessage());
        LoaderClient loaderWithFakeTaskManager = getFakeTimeoutLoader();
        loaderWithFakeTaskManager.waitToFinish();
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

    private long loadAndTime(){
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

        loader.waitToFinish();

        long duration = System.currentTimeMillis() - startTime;
        System.out.println("Time to load: " + duration);

        Collection<Entity> nameTags = graph.getEntityType("name_tag").instances();

        assertEquals(50, nameTags.size());
        ids.stream().map(graph::getResourcesByValue).forEach(Assert::assertNotNull);

        return duration;
    }

    private LoaderClient getFakeNormalLoader() {
        return new LoaderClient(graph.getKeyspace(), Grakn.DEFAULT_URI);
    }

    private LoaderClient getFakeTimeoutLoader() {
        TaskManager fakeClusterManager = getFakeTaskManager(invocation -> TaskStatus.CREATED);
        return new LoaderClient(graph.getKeyspace(), Grakn.DEFAULT_URI);
    }

    private TaskManager getFakeTaskManager(Answer answer) {
        TaskState fakeTask = new TaskState(LoaderTask.class.getName());
        String fakeTaskId = fakeTask.getId();
        StandaloneTaskManager fakeTaskManager = mock(StandaloneTaskManager.class);
        TaskStateGraphStore fakeStorage = mock(TaskStateGraphStore.class);
        TaskState fakeTaskState = mock(TaskState.class);
        when(fakeTaskManager.storage()).thenReturn(fakeStorage);
        when(fakeStorage.getState(fakeTaskId)).thenReturn(fakeTaskState);
        when(fakeStorage.getState(fakeTaskId).status()).thenAnswer(answer);
        Set<TaskState> fakeTasks = new HashSet<>();
        fakeTasks.add(fakeTask);
        when(fakeStorage.getTasks(null, LoaderTask.class.getName(), graph.getKeyspace(), 100000, 0)).thenReturn(fakeTasks);
        return fakeTaskManager;
    }
}
