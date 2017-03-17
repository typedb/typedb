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
import ai.grakn.engine.tasks.storage.TaskStateInMemoryStore;
import ai.grakn.engine.util.EngineID;
import ai.grakn.engine.tasks.mock.ShortExecutionMockTask;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.engine.TaskStatus.CREATED;
import static ai.grakn.engine.TaskStatus.RUNNING;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TaskStateInMemoryStoreTest {
    private static final EngineID engineID = EngineID.me();
    private TaskStateStorage stateStorage;

    @Before
    public void setUp() {
        stateStorage = new TaskStateInMemoryStore();
    }

    @Test
    public void testInsertNewState() {
        TaskId id = stateStorage.newState(task());
        assertNotNull(id);

        TaskState state = stateStorage.getState(id);
        assertEquals("name", ShortExecutionMockTask.class, state.taskClass());
        assertEquals("creator", this.getClass().getName(), state.creator());
        assertEquals("recurring", false, state.schedule().isRecurring());
        assertEquals("interval", Optional.empty(), state.schedule().interval());
    }

    @Test
    public void testUpdateTaskState() {
        TaskId id = stateStorage.newState(task());
        assertNotNull(id);

        // Get current values
        TaskState state = stateStorage.getState(id);
        assertEquals(CREATED, state.status());

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
        assertEquals(getFullStackTrace(exception), newState.stackTrace());
    }

    @Test
    public void testGetTasksByStatus() {
        TaskId id = stateStorage.newState(task());
        Set<TaskState> res = stateStorage.getTasks(CREATED, null, null, null, 0, 0);

        assertTrue(res.parallelStream()
                        .map(TaskState::getId)
                        .filter(x -> x.equals(id))
                        .collect(Collectors.toList())
                        .size() == 1);
    }

    @Test
    public void testGetTasksByCreator() {
        TaskId id = stateStorage.newState(task());
        Set<TaskState> res = stateStorage.getTasks(null, null, this.getClass().getName(), null, 0, 0);

        assertTrue(res.parallelStream()
                .map(TaskState::getId)
                        .filter(x -> x.equals(id))
                        .collect(Collectors.toList())
                        .size() == 1);
    }

    @Test
    public void testGetTasksByClassName() {
        TaskId id = stateStorage.newState(task());
        Set<TaskState> res = stateStorage.getTasks(null, ShortExecutionMockTask.class.getName(), null, null, 0, 0);

        assertTrue(res.parallelStream()
                .map(TaskState::getId)
                .filter(x -> x.equals(id))
                .collect(Collectors.toList())
                .size() == 1);
    }

    @Test
    public void testGetAllTasks() {
        TaskId id = stateStorage.newState(task());
        Set<TaskState> res = stateStorage.getTasks(null, null, null, null, 0, 0);

        assertTrue(res.parallelStream()
                .map(TaskState::getId)
                .filter(x -> x.equals(id))
                .collect(Collectors.toList())
                .size() == 1);
    }

    @Test
    public void testGetByRunningEngine(){
        EngineID me = EngineID.me();
        TaskId id = stateStorage.newState(task().markRunning(me));
        Set<TaskState > res = stateStorage.getTasks(null, null, null, me, 1, 0);
        TaskState resultant = res.iterator().next();
        assertEquals(resultant.getId(), id);
        assertEquals(resultant.engineID(), me);
    }

    @Test
    public void testTaskPagination() {
        for (int i = 0; i < 20; i++) {
            stateStorage.newState(task());
        }

        Set<TaskState> setA = stateStorage.getTasks(null, null, null, null, 10, 0);
        Set<TaskState> setB = stateStorage.getTasks(null, null, null, null, 10, 10);

        setA.forEach(x -> assertFalse(setB.contains(x)));
    }

    public TaskState task(){
        return TaskState.of(ShortExecutionMockTask.class, this.getClass().getName(), TaskSchedule.now(), null);
    }
}
