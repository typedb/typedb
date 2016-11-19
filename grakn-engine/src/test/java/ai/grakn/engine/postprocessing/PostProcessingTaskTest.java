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

package ai.grakn.engine.postprocessing;

import ai.grakn.engine.backgroundtasks.InMemoryTaskManager;
import ai.grakn.engine.GraknEngineTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;

import static ai.grakn.engine.backgroundtasks.TaskStatus.COMPLETED;
import static ai.grakn.engine.backgroundtasks.TaskStatus.CREATED;
import static ai.grakn.engine.backgroundtasks.TaskStatus.STOPPED;

public class PostProcessingTaskTest extends GraknEngineTestBase {
    private InMemoryTaskManager taskManager;

    @Before
    public void setUp() {
        taskManager = InMemoryTaskManager.getInstance();
    }

    @Test
    public void testStart() throws Exception {
        String id= taskManager.scheduleTask(new PostProcessingTask(), this.getClass().getName(), new Date(), 0, null);
        Assert.assertNotEquals(CREATED, taskManager.storage().getState(id).status());

        // Wait for supervisor thread to mark task as completed
        final long initial = new Date().getTime();

        while ((new Date().getTime())-initial < 10000) {
            if (taskManager.storage().getState(id).status() == COMPLETED)
                break;

            Thread.sleep(100);
        }

        // Check that task has ran
        Assert.assertEquals(COMPLETED, taskManager.storage().getState(id).status());
    }

    @Test
    public void testStop() {
        String id = taskManager.scheduleTask(new PostProcessingTask(), this.getClass().getName(), new Date(), 10000, null);
        taskManager.stopTask(id, this.getClass().getName());
        Assert.assertEquals(STOPPED, taskManager.storage().getState(id).status());
    }
}
