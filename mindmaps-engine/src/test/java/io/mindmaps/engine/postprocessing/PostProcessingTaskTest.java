/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.engine.postprocessing;

import io.mindmaps.engine.MindmapsEngineTestBase;
import io.mindmaps.engine.backgroundtasks.InMemoryTaskManager;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static io.mindmaps.engine.backgroundtasks.TaskStatus.*;
import static io.mindmaps.engine.backgroundtasks.TaskStatus.RUNNING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class PostProcessingTaskTest extends MindmapsEngineTestBase {
    private InMemoryTaskManager taskManager;

    @Before
    public void setUp() {
        taskManager = InMemoryTaskManager.getInstance();
    }

    @Test
    public void testStart() throws Exception {
        String id = taskManager.scheduleTask(new PostProcessingTask(), 100);
        assertNotEquals(CREATED, taskManager.getTaskState(id).getStatus());

        // Wait for supervisor thread to mark task as completed
        Thread.sleep(2000);

        // Check that task has ran
        assertEquals(COMPLETED, taskManager.getTaskState(id).getStatus());
    }

    @Test
    public void testStop() {
        String id = taskManager.scheduleRecurringTask(new PostProcessingTask(), 100, 10000);
        taskManager.stopTask(id, null, null);
        assertEquals(STOPPED, taskManager.getTaskState(id).getStatus());
    }

    @Test
    public void testPauseResume() {
        String id = taskManager.scheduleTask(new PostProcessingTask(), 10000);
        assertNotEquals(CREATED, taskManager.getTaskState(id).getStatus());

        taskManager.pauseTask(id, null, null);
        assertEquals(PAUSED, taskManager.getTaskState(id).getStatus());

        taskManager.resumeTask(id, null, null);
        assertEquals(SCHEDULED, taskManager.getTaskState(id).getStatus());
    }

    // There is a concurrency issue with this test that I currently cannot fix
    @Ignore
    public void testRestart() {
        String id = taskManager.scheduleTask(new PostProcessingTask(), 0);
        taskManager.stopTask(id, null, null);
        assertEquals(STOPPED, taskManager.getTaskState(id).getStatus());
        taskManager.restartTask(id, null, null);
        assertNotEquals(STOPPED, taskManager.getTaskState(id).getStatus());
    }
}
