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

package ai.grakn.test.engine.tasks.storage;

import ai.grakn.engine.TaskId;
import ai.grakn.engine.tasks.TaskSchedule;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.engine.tasks.storage.TaskStateGraphStore;
import ai.grakn.engine.util.EngineID;
import ai.grakn.test.EngineContext;
import ai.grakn.engine.tasks.mock.ShortExecutionMockTask;
import mjson.Json;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static ai.grakn.engine.TaskStatus.CREATED;
import static ai.grakn.engine.TaskStatus.RUNNING;
import static ai.grakn.engine.tasks.TaskSchedule.at;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TaskStateGraphStoreTest {
    private static final EngineID engineID = EngineID.me();
    private TaskStateStorage stateStorage;

    @ClassRule
    public static final EngineContext engine = EngineContext.startInMemoryServer();

    @Before
    public void setUp() {
        stateStorage = new TaskStateGraphStore();
    }

    @Test
    public void testTaskStateStoreRetrieve() {
        ShortExecutionMockTask task = new ShortExecutionMockTask();
        Instant runAt = Instant.now();
        Json configuration = Json.object("test key", "test value");

        TaskId id = stateStorage.newState(task(at(runAt), configuration));
        assertNotNull(id);

        TaskState state = stateStorage.getState(id);
        assertNotNull(state);

        Assert.assertEquals(task.getClass(), state.taskClass());
        Assert.assertEquals(this.getClass().getName(), state.creator());
        assertEquals(runAt, state.schedule().runAt());
        assertFalse(state.schedule().isRecurring());
        assertEquals(Optional.empty(), state.schedule().interval());
        assertEquals(configuration.toString(), state.configuration().toString());
    }

    @Test
    public void testUpdateTaskState() {
        Instant runAt = Instant.now();
        Json configuration = Json.object("test key", "test value");

        TaskId id = stateStorage.newState(task(at(runAt), configuration));
        assertNotNull(id);

        // Get current values
        TaskState state = stateStorage.getState(id);

        // Change
        state.markRunning(engineID);
        stateStorage.updateState(state);

        TaskState newState = stateStorage.getState(id);
        assertEquals(RUNNING, newState.status());
        assertEquals(engineID, newState.engineID());

        // Change
        Exception exception = new UnsupportedOperationException("message");
        newState.markFailed(exception);
        stateStorage.updateState(newState);

        newState = stateStorage.getState(id);
        assertEquals(exception.getClass().getName(), newState.exception());
        assertEquals(getFullStackTrace(exception), newState.stackTrace());
    }

    @Test
    public void testGetTaskStateByStatus() {
        TaskId id = stateStorage.newState(task().markScheduled());
        assertNotNull(id);

        Set<TaskState> res = stateStorage.getTasks(CREATED, null, null, null, 0, 0);
        assertTrue(res.parallelStream()
                .map(TaskState::getId)
                .filter(x -> x.equals(id))
                .collect(Collectors.toList())
                .size() == 1);
    }

    @Test
    public void testGetTaskStateByCreator() {
        TaskId id = stateStorage.newState(task(TaskSchedule.now(), Json.object(), "other"));
        assertNotNull(id);

        Set<TaskState> res = stateStorage.getTasks(null, null, "other", null, 0, 0);
        assertTrue(res.parallelStream()
                .map(TaskState::getId)
                        .filter(x -> x.equals(id))
                        .collect(Collectors.toList())
                        .size() == 1);
    }

    @Test
    public void testGetTaskStateByClassName() {
        TaskId id = stateStorage.newState(task());
        assertNotNull(id);

        Set<TaskState> res = stateStorage.getTasks(null, ShortExecutionMockTask.class.getName(), null, null, 0, 0);
        assertTrue(res.parallelStream()
                .map(TaskState::getId)
                .filter(x -> x.equals(id))
                .collect(Collectors.toList())
                .size() == 1);
    }

    @Test
    public void testGetAllTaskStates() {
        int sizeBeforeAdding = stateStorage.getTasks(null, null, null, null, 0, 0).size();

        int numberTasks = 10;
        IntStream.range(0, numberTasks)
                .mapToObj(i -> task())
                .forEach(stateStorage::newState);

        Set<TaskState> res = stateStorage.getTasks(null, null, null,null, 0, 0);
        assertEquals(sizeBeforeAdding + numberTasks, res.size());
    }

    @Test
    public void testTaskStatePagination() {
        for (int i = 0; i < 10; i++) {
            stateStorage.newState(task());
        }

        Set<TaskState> setA = stateStorage.getTasks(null, null, null, null, 5, 0);
        Set<TaskState> setB = stateStorage.getTasks(null, null, null, null, 5, 5);

        setA.forEach(x -> assertFalse(setB.contains(x)));
    }

    @Test
    public void testGetByRunningEngine(){
        EngineID engine1 = EngineID.me();
        TaskId id = stateStorage.newState(task().markRunning(engine1));

        Set<TaskState> res = stateStorage.getTasks(null, null, null, engine1, 1, 0);
        TaskState resultant = res.iterator().next();
        assertEquals(resultant.getId(), id);
        assertEquals(resultant.engineID(), engine1);

        EngineID engine2 = EngineID.me();
        stateStorage.updateState(resultant.markRunning(engine2));
        resultant = stateStorage.getTasks(null, null, null, engine2, 1, 0).iterator().next();
        assertEquals(resultant.getId(), id);
        assertEquals(resultant.engineID(), engine2);
    }

    public TaskState task(){
        return task(TaskSchedule.now(), Json.object(), getClass().getName());
    }

    public TaskState task(TaskSchedule schedule, Json configuration){
        return task(schedule, configuration, getClass().getName());
    }

    public TaskState task(TaskSchedule schedule, Json configuration, String creator){
        return TaskState.of(ShortExecutionMockTask.class, creator, schedule, configuration);
    }
}
