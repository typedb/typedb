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
import io.mindmaps.engine.backgroundtasks.TaskStatus;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

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
        UUID uuid = taskManager.scheduleTask(new PostProcessingTask(), 100);
        assertNotEquals(TaskStatus.CREATED, taskManager.getTaskState(uuid).getStatus());

        // Wait for supervisor thread to mark task as completed
        Thread.sleep(2000);

        // Check that task has ran
        assertEquals(TaskStatus.COMPLETED, taskManager.getTaskState(uuid).getStatus());
    }

    @Test
    public void testStop() {
        UUID uuid = taskManager.scheduleRecurringTask(new PostProcessingTask(), 100, 10000);
        taskManager.stopTask(uuid);
        assertEquals(TaskStatus.STOPPED, taskManager.getTaskState(uuid).getStatus());
    }

    @Test
    public void testPauseResume() {
        UUID uuid = taskManager.scheduleTask(new PostProcessingTask(), 1000);
        assertNotEquals(TaskStatus.CREATED, taskManager.getTaskState(uuid).getStatus());

        taskManager.pauseTask(uuid);
        assertEquals(TaskStatus.PAUSED, taskManager.getTaskState(uuid).getStatus());

        taskManager.resumeTask(uuid);
        assertEquals(TaskStatus.RUNNING, taskManager.getTaskState(uuid).getStatus());
    }

    @Test
    public void testRestart() throws Exception {
        UUID uuid = taskManager.scheduleTask(new PostProcessingTask(), 0);
        taskManager.stopTask(uuid);
        taskManager.restartTask(uuid);
        Thread.sleep(100);
        assertEquals(TaskStatus.RUNNING, taskManager.getTaskState(uuid).getStatus());
    }
}
