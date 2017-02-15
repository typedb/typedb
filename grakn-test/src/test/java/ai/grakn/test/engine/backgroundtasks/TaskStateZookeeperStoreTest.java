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

package ai.grakn.test.engine.backgroundtasks;

import ai.grakn.engine.backgroundtasks.TaskStateStorage;
import ai.grakn.engine.backgroundtasks.TaskState;
import ai.grakn.engine.backgroundtasks.distributed.ZookeeperConnection;
import ai.grakn.engine.backgroundtasks.taskstatestorage.TaskStateZookeeperStore;
import ai.grakn.test.EngineContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static ai.grakn.engine.TaskStatus.CREATED;
import static ai.grakn.engine.TaskStatus.SCHEDULED;
import static java.time.Instant.now;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TaskStateZookeeperStoreTest {
    private static ZookeeperConnection connection;
    private static TaskStateStorage stateStorage;

    @ClassRule
    public static final EngineContext engine = EngineContext.startKafkaServer();

    @BeforeClass
    public static void setUp() throws Exception {
        connection = new ZookeeperConnection();
        stateStorage = new TaskStateZookeeperStore(connection);
    }

    @AfterClass
    public static void teardown(){
        connection.close();
    }

    @Test
    public void testStoreRetrieve() throws Exception {
        String id = stateStorage.newState(task());

        // Retrieve
        TaskState state = stateStorage.getState(id);
        assertNotNull(state);
        assertEquals(CREATED, state.status());
        assertEquals(TestTask.class.getName(), state.taskClassName());
        assertEquals(this.getClass().getName(), state.creator());
    }

    @Test
    public void testUpdate() throws Exception {
        String id = stateStorage.newState(task());

        // Change
        String engineID = UUID.randomUUID().toString();
        String checkpoint = "test checkpoint";

        TaskState state = stateStorage.getState(id)
                .status(SCHEDULED)
                .engineID(engineID)
                .checkpoint(checkpoint);

        stateStorage.updateState(state);

        state = stateStorage.getState(id);
        assertEquals(SCHEDULED, state.status());
        assertEquals(engineID, state.engineID());
        assertEquals(checkpoint, state.checkpoint());
    }

    @Test
    public void testUpdateInvalid() throws Exception {
        String id = stateStorage.newState(task());

        // update
        String engineID = UUID.randomUUID().toString();
        String checkpoint = "test checkpoint";

        TaskState state = stateStorage.getState(id);
        stateStorage.updateState(state.engineID(engineID).checkpoint(checkpoint));

        // get again
        state = stateStorage.getState(id);
        assertEquals(CREATED, state.status());
        assertEquals(engineID, state.engineID());
        assertEquals(checkpoint, state.checkpoint());
    }


    @Test
    public void testGetTasksByStatus() {
        String id = stateStorage.newState(task());
        stateStorage.newState(task().status(SCHEDULED));
        Set<TaskState> res = stateStorage.getTasks(CREATED, null, null, 0, 0);

        assertTrue(res.parallelStream()
                .map(TaskState::getId)
                .filter(x -> x.equals(id))
                .collect(Collectors.toList())
                .size() == 1);
    }

    @Test
    public void testGetTasksByCreator() {
        String id = stateStorage.newState(task());
        stateStorage.newState(task().creator("another"));
        Set<TaskState> res = stateStorage.getTasks(null, null, this.getClass().getName(), 0, 0);

        assertTrue(res.parallelStream()
                .map(TaskState::getId)
                .filter(x -> x.equals(id))
                .collect(Collectors.toList())
                .size() == 1);
    }

    @Test
    public void testGetTasksByClassName() {
        String id = stateStorage.newState(task());
        Set<TaskState> res = stateStorage.getTasks(null, TestTask.class.getName(), null, 0, 0);

        assertTrue(res.parallelStream()
                .map(TaskState::getId)
                .filter(x -> x.equals(id))
                .collect(Collectors.toList())
                .size() == 1);
    }

    @Test
    public void testGetAllTasks() {
        String id = stateStorage.newState(task());
        Set<TaskState> res = stateStorage.getTasks(null, null, null, 0, 0);

        assertTrue(res.parallelStream()
                .map(TaskState::getId)
                .filter(x -> x.equals(id))
                .collect(Collectors.toList())
                .size() == 1);
    }

    @Test
    public void testTaskPagination() {
        for (int i = 0; i < 20; i++) {
            stateStorage.newState(task());
        }

        Set<TaskState> setA = stateStorage.getTasks(null, null, null, 10, 0);
        Set<TaskState> setB = stateStorage.getTasks(null, null, null, 10, 10);

        setA.forEach(x -> assertFalse(setB.contains(x)));
    }

    public TaskState task(){
        return new TaskState(TestTask.class.getName())
                .creator(this.getClass().getName())
                .statusChangedBy(this.getClass().getName())
                .runAt(now())
                .isRecurring(false)
                .interval(0)
                .configuration(null);
    }
}
