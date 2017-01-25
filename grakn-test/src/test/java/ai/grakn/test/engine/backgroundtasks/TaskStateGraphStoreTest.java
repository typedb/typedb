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
import ai.grakn.engine.backgroundtasks.taskstorage.TaskStateGraphStore;
import ai.grakn.test.EngineContext;
import javafx.util.Pair;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static ai.grakn.engine.backgroundtasks.TaskStatus.CREATED;
import static ai.grakn.engine.backgroundtasks.TaskStatus.SCHEDULED;
import static java.time.Instant.now;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TaskStateGraphStoreTest {
    private TaskStateStorage stateStorage;

    @ClassRule
    public static final EngineContext engine = EngineContext.startServer();

    @Before
    public void setUp() {
        stateStorage = new TaskStateGraphStore();
    }

    @Test
    public void testStoreRetrieve() {
        TestTask task = new TestTask();
        Instant runAt = Instant.now();
        JSONObject configuration = new JSONObject().put("test key", "test value");

        String id = stateStorage.newState(task().configuration(configuration).runAt(runAt));
        assertNotNull(id);

        TaskState state = stateStorage.getState(id);
        assertNotNull(state);

        Assert.assertEquals(task.getClass().getName(), state.taskClassName());
        Assert.assertEquals(this.getClass().getName(), state.creator());
        assertEquals(runAt, state.runAt());
        assertFalse(state.isRecurring());
        assertEquals(0, state.interval());
        assertEquals(configuration.toString(), state.configuration().toString());
    }

    @Test
    public void testUpdate() {
        Instant runAt = Instant.now();
        JSONObject configuration = new JSONObject().put("key", "test value");

        String id = stateStorage.newState(task().configuration(configuration).runAt(runAt));
        assertNotNull(id);

        // Get current values
        TaskState state = stateStorage.getState(id);
        TaskState midState = stateStorage.getState(id);

        String stackTrace = getFullStackTrace(new UnsupportedOperationException());

        // Change.
        stateStorage.updateState(midState
                .status(SCHEDULED)
                .statusChangedBy("bla")
                .engineID(UUID.randomUUID().toString())
                .checkpoint("checkpoint")
                .exception(stackTrace));

        TaskState newState = stateStorage.getState(id);
        assertNotEquals(state, newState);
        assertNotEquals(state.status(), newState.status());
        assertNotEquals(state.statusChangedBy(), newState.statusChangedBy());
        assertNotEquals(state.engineID(), newState.engineID());
        assertEquals(stackTrace, newState.exception());
        assertEquals("checkpoint", newState.checkpoint());
        assertEquals(state.configuration().toString(), newState.configuration().toString());
    }

    @Test
    public void testGetByStatus() {
        String id = stateStorage.newState(task());
        assertNotNull(id);

        Set<Pair<String, TaskState>> res = stateStorage.getTasks(CREATED, null, null, 0, 0);
        assertTrue(res.parallelStream()
                .map(Pair::getKey)
                .filter(x -> x.equals(id))
                .collect(Collectors.toList())
                .size() == 1);
    }

    @Test
    public void testGetByCreator() {
        String id = stateStorage.newState(task());
        assertNotNull(id);

        Set<Pair<String, TaskState>> res = stateStorage.getTasks(null, null, this.getClass().getName(), 0, 0);
        assertTrue(res.parallelStream()
                        .map(Pair::getKey)
                        .filter(x -> x.equals(id))
                        .collect(Collectors.toList())
                        .size() == 1);
    }

    @Test
    public void testGetByClassName() {
        String id = stateStorage.newState(task());
        assertNotNull(id);

        Set<Pair<String, TaskState>> res = stateStorage.getTasks(null, TestTask.class.getName(), null, 0, 0);
        assertTrue(res.parallelStream()
                        .map(Pair::getKey)
                        .filter(x -> x.equals(id))
                        .collect(Collectors.toList())
                        .size() == 1);
    }

    @Test
    public void testGetAll() {
        String id = stateStorage.newState(task());
        assertNotNull(id);

        Set<Pair<String, TaskState>> res = stateStorage.getTasks(null, null, null, 0, 0);
        assertTrue(res.parallelStream()
                        .map(Pair::getKey)
                        .filter(x -> x.equals(id))
                        .collect(Collectors.toList())
                        .size() == 1);
    }

    @Test
    public void testPagination() {
        for (int i = 0; i < 10; i++) {
            stateStorage.newState(task());
        }

        Set<Pair<String, TaskState>> setA = stateStorage.getTasks(null, null, null, 5, 0);
        Set<Pair<String, TaskState>> setB = stateStorage.getTasks(null, null, null, 5, 5);

        setA.forEach(x -> assertFalse(setB.contains(x)));
    }

    public TaskState task(){
        return new TaskState(TestTask.class.getName())
                .creator(this.getClass().getName())
                .statusChangedBy(this.getClass().getName())
                .runAt(now())
                .isRecurring(false)
                .interval(0)
                .engineID(UUID.randomUUID().toString())
                .configuration(null);
    }
}
