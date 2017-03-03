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
import ai.grakn.test.engine.tasks.ShortExecutionTestTask;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.engine.TaskStatus.CREATED;
import static ai.grakn.engine.TaskStatus.SCHEDULED;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TaskStateInMemoryStoreTest {
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
        assertEquals("name", ShortExecutionTestTask.class, state.taskClass());
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
        assertEquals(this.getClass().getName(), state.statusChangedBy());

        String exception = getFullStackTrace(new UnsupportedOperationException("message"));

        // Change
        state.status(SCHEDULED)
                .statusChangedBy("bla")
                .engineID("newEngine")
                .exception(exception);

        stateStorage.updateState(state);

        TaskState newState = stateStorage.getState(id);
        assertEquals(SCHEDULED, newState.status());
        assertEquals("bla", newState.statusChangedBy());
        assertEquals("newEngine", newState.engineID());
        assertEquals(exception, newState.exception());
    }

    @Test
    public void testGetTasksByStatus() {
        TaskId id = stateStorage.newState(task());
        Set<TaskState> res = stateStorage.getTasks(CREATED, null, null, 0, 0);

        assertTrue(res.parallelStream()
                        .map(TaskState::getId)
                        .filter(x -> x.equals(id))
                        .collect(Collectors.toList())
                        .size() == 1);
    }

    @Test
    public void testGetTasksByCreator() {
        TaskId id = stateStorage.newState(task());
        Set<TaskState> res = stateStorage.getTasks(null, null, this.getClass().getName(), 0, 0);

        assertTrue(res.parallelStream()
                .map(TaskState::getId)
                        .filter(x -> x.equals(id))
                        .collect(Collectors.toList())
                        .size() == 1);
    }

    @Test
    public void testGetTasksByClassName() {
        TaskId id = stateStorage.newState(task());
        Set<TaskState> res = stateStorage.getTasks(null, ShortExecutionTestTask.class.getName(), null, 0, 0);

        assertTrue(res.parallelStream()
                .map(TaskState::getId)
                .filter(x -> x.equals(id))
                .collect(Collectors.toList())
                .size() == 1);
    }

    @Test
    public void testGetAllTasks() {
        TaskId id = stateStorage.newState(task());
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
        return TaskState.of(ShortExecutionTestTask.class, this.getClass().getName(), TaskSchedule.now(), null)
                .statusChangedBy(this.getClass().getName());
    }
}
