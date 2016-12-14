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

import ai.grakn.engine.backgroundtasks.taskstorage.SynchronizedState;
import ai.grakn.engine.backgroundtasks.taskstorage.SynchronizedStateStorage;
import ai.grakn.test.AbstractEngineTest;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import static ai.grakn.engine.backgroundtasks.TaskStatus.CREATED;
import static ai.grakn.engine.backgroundtasks.TaskStatus.SCHEDULED;
import static org.junit.Assert.*;

public class SynchronizedStateStorageTest extends AbstractEngineTest {
    private SynchronizedStateStorage stateStorage;

    @Before
    public void setUp() throws Exception {
        stateStorage = SynchronizedStateStorage.getInstance();
    }

    @Test
    public void testStoreRetrieve() {
        String id = UUID.randomUUID().toString();
        String engineID = UUID.randomUUID().toString();
        String checkpoint = "test checkpoint";

        stateStorage.newState(id, CREATED, engineID, checkpoint);

        // Retrieve
        SynchronizedState state = stateStorage.getState(id);
        assertNotNull(state);
        assertEquals(CREATED, state.status());
        assertEquals(engineID, state.engineID());
        assertEquals(checkpoint, state.checkpoint());
    }

    @Test
    public void testStoreWithPartial() {
        String id = UUID.randomUUID().toString();
        stateStorage.newState(id, CREATED, null, null);

        // Retrieve
        SynchronizedState state = stateStorage.getState(id);
        assertNotNull(state);
        assertEquals(CREATED, state.status());
    }

    @Test
    public void testUpdate() {
        String id = UUID.randomUUID().toString();
        String engineID = UUID.randomUUID().toString();
        String checkpoint = "test checkpoint";
        stateStorage.newState(id, CREATED, engineID, checkpoint);

        // Get current values
        SynchronizedState state = stateStorage.getState(id);

        // Change.
        stateStorage.updateState(id, SCHEDULED, "bla", "example.com");

        SynchronizedState newState = stateStorage.getState(id);

        assertNotEquals("the object itself", state, newState);
        assertNotEquals("status", state.status(), newState.status());
        assertNotEquals("engine id", state.engineID(), newState.engineID());
        assertNotEquals("checkpoint", state.checkpoint(), newState.checkpoint());
    }

    @Test
    public void testUpdateInvalid() {
        String id = UUID.randomUUID().toString();
        String engineID = UUID.randomUUID().toString();
        String checkpoint = "test checkpoint";
        stateStorage.newState(id, CREATED, engineID, checkpoint);

        stateStorage.updateState(null, SCHEDULED, "bla", "example.com");
        SynchronizedState state = stateStorage.getState(id);
        assertEquals(CREATED, state.status());

        stateStorage.updateState(id, null, null, null);
        state = stateStorage.getState(id);
        assertEquals(CREATED, state.status());
    }
}
