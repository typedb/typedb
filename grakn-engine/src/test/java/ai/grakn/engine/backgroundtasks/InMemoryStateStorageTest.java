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

package ai.grakn.engine.backgroundtasks;

import javafx.util.Pair;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.engine.backgroundtasks.TaskStatus.CREATED;
import static ai.grakn.engine.backgroundtasks.TaskStatus.SCHEDULED;
import static org.junit.Assert.*;

public class InMemoryStateStorageTest {
    private StateStorage stateStorage;

    @Before
    public void setUp() {
        stateStorage = InMemoryStateStorage.getInstance();
    }

    @Test
    public void testNewState() {
        TestTask task = new TestTask();

        Date runAt = new Date();
        String id = stateStorage.newState(task.getClass().getName(), this.getClass().getName(), runAt, false, 0, null);
        assertNotNull(id);

        TaskState state = stateStorage.getState(id);
        assertEquals("name", task.getClass().getName(), state.taskClassName());
        assertEquals("creator", this.getClass().getName(), state.creator());
        assertEquals("runAt", runAt, state.runAt());
        assertEquals("recurring", false, state.isRecurring());
        assertEquals("interval", 0, state.interval());
    }

    @Test
    public void testUpdateState() {
        TestTask task = new TestTask();
        Date runAt = new Date();

        String id = stateStorage.newState(task.getClass().getName(), this.getClass().getName(), runAt, false, 0, null);
        assertNotNull(id);

        // Get current values
        TaskState state = stateStorage.getState(id);

        // Change.
        stateStorage.updateState(id, SCHEDULED, "bla", "example.com", new UnsupportedOperationException("message"), "blabla", null);

        TaskState newState = stateStorage.getState(id);
        assertNotEquals("the object itself", state, newState);
        assertNotEquals("status", state.status(), newState.status());
        assertNotEquals("status changed by", state.statusChangedBy(), newState.statusChangedBy());
        assertNotEquals("hostname", state.executingHostname(), newState.executingHostname());
        assertNotEquals("exception message", state.exception(), newState.exception());
        assertNotEquals("stack trace", state.stackTrace(), newState.stackTrace());
        assertNotEquals("checkpoint", state.checkpoint(), newState.checkpoint());
    }

    @Test
    public void testGetByStatus() {
        String id = stateStorage.newState(TestTask.class.getName(), this.getClass().getName(), new Date(), false, 0, null);
        Set<Pair<String, TaskState>> res = stateStorage.getTasks(CREATED, null, null, 0, 0);

        assertTrue(res.parallelStream()
                        .map(Pair::getKey)
                        .filter(x -> x.equals(id))
                        .collect(Collectors.toList())
                        .size() == 1);
    }

    @Test
    public void testGetByCreator() {
        String id = stateStorage.newState(TestTask.class.getName(), this.getClass().getName(), new Date(), false, 0, null);
        Set<Pair<String, TaskState>> res = stateStorage.getTasks(null, null, this.getClass().getName(), 0, 0);

        assertTrue(res.parallelStream()
                        .map(Pair::getKey)
                        .filter(x -> x.equals(id))
                        .collect(Collectors.toList())
                        .size() == 1);
    }

    @Test
    public void testGetByClassName() {
        String id = stateStorage.newState(TestTask.class.getName(), this.getClass().getName(), new Date(), false, 0, null);
        Set<Pair<String, TaskState>> res = stateStorage.getTasks(null, TestTask.class.getName(), null, 0, 0);

        assertTrue(res.parallelStream()
                        .map(Pair::getKey)
                        .filter(x -> x.equals(id))
                        .collect(Collectors.toList())
                        .size() == 1);
    }

    @Test
    public void testGetAll() {
        String id = stateStorage.newState(TestTask.class.getName(), this.getClass().getName(), new Date(), false, 0, null);
        Set<Pair<String, TaskState>> res = stateStorage.getTasks(null, null, null, 0, 0);

        assertTrue(res.parallelStream()
                        .map(Pair::getKey)
                        .filter(x -> x.equals(id))
                        .collect(Collectors.toList())
                        .size() == 1);
    }

    @Test
    public void testPagination() {
        for (int i = 0; i < 20; i++) {
            stateStorage.newState(TestTask.class.getName(), this.getClass().getName(), new Date(), false, 0, null);
        }

        Set<Pair<String, TaskState>> setA = stateStorage.getTasks(null, null, null, 10, 0);
        Set<Pair<String, TaskState>> setB = stateStorage.getTasks(null, null, null, 10, 10);

        setA.forEach(x -> assertFalse(setB.contains(x)));
    }
}
