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
    private TaskStateStorage stateStorage;

    @ClassRule
    public static final EngineContext engine = EngineContext.startServer();

    @Before
    public void setUp() throws Exception {
        stateStorage = new ZookeeperStateStorage(engine.getClusterManager().getZookeeperConnection());
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
