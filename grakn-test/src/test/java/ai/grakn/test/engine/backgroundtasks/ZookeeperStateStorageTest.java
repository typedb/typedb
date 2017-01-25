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

import ai.grakn.engine.backgroundtasks.TaskState;
import ai.grakn.engine.backgroundtasks.taskstorage.ZookeeperStateStorage;
import ai.grakn.test.EngineContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.UUID;

import static ai.grakn.engine.backgroundtasks.TaskStatus.CREATED;
import static ai.grakn.engine.backgroundtasks.TaskStatus.SCHEDULED;
import static java.time.Instant.now;
import static junit.framework.Assert.assertNull;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ZookeeperStateStorageTest {
    private ZookeeperStateStorage stateStorage;

    @ClassRule
    public static final EngineContext engine = EngineContext.startServer();

    @Before
    public void setUp() throws Exception {
        stateStorage = engine.getClusterManager().getStorage();
    }

    @Test
    public void testStoreRetrieve() throws Exception {
        String id = stateStorage.newState(TestTask.class.getName(), this.getClass().getName(), now(), false, 0, null);

        // Retrieve
        TaskState state = stateStorage.getState(id);
        assertNotNull(state);
        assertEquals(CREATED, state.status());
        assertEquals(TestTask.class.getName(), state.taskClassName());
        assertEquals(this.getClass().getName(), state.creator());
    }

    @Test
    public void testStoreWithPartial() throws Exception {
        String id = stateStorage.newState(TestTask.class.getName(), this.getClass().getName(), now(), null, 0, null);
        assertNull(id);
    }

    @Test
    public void testUpdate() throws Exception {
        String id = stateStorage.newState(TestTask.class.getName(), this.getClass().getName(), now(), false, 0, null);

        // Change
        String engineID = UUID.randomUUID().toString();
        String checkpoint = "test checkpoint";
        stateStorage.updateState(id, SCHEDULED, "bla", engineID, null, checkpoint, null);

        TaskState state = stateStorage.getState(id);
        assertEquals(SCHEDULED, state.status());
        assertEquals(engineID, state.engineID());
        assertEquals(checkpoint, state.checkpoint());
    }

    @Test
    public void testUpdateInvalid() throws Exception {
        String id = stateStorage.newState(TestTask.class.getName(), this.getClass().getName(), now(), false, 0, null);

        // update
        String engineID = UUID.randomUUID().toString();
        String checkpoint = "test checkpoint";
        stateStorage.updateState(null, SCHEDULED, "bla", engineID, null, checkpoint, null);

        // get again
        TaskState state = stateStorage.getState(id);
        assertEquals(CREATED, state.status());

        stateStorage.updateState(id, null, null, null, null, null, null);
        state = stateStorage.getState(id);
        assertEquals(CREATED, state.status());
    }
}
