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

package io.mindmaps.engine.backgroundtasks;

import io.mindmaps.engine.MindmapsEngineTestBase;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static io.mindmaps.engine.backgroundtasks.TaskStatus.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class InMemoryTaskManagerTest extends MindmapsEngineTestBase {
    private InMemoryTaskManager taskManager;
    private static long TASK_DELAY = 100000;

    @Before
    public void setUp() {
        taskManager = InMemoryTaskManager.getInstance();
    }

    @Test
    public void testQueueAndRetrieve() throws Exception {
        TestTask task = new TestTask();
        int runCount = task.getRunCount();

        String id = taskManager.scheduleTask(task, 0);
        assertNotEquals(TaskStatus.CREATED, taskManager.getTaskState(id).getStatus());

        // Wait for supervisor thread to mark task as completed
        Thread.sleep(2000);

        // Check that task has ran
        assertEquals(TaskStatus.COMPLETED, taskManager.getTaskState(id).getStatus());
        assertTrue(task.getRunCount() > runCount);
    }

    @Test
    public void testRecurring() throws Exception {
        TestTask task = new TestTask();
        String id = taskManager.scheduleRecurringTask(task, 100, 100);
        assertNotEquals(TaskStatus.CREATED, taskManager.getTaskState(id).getStatus());

        // Check that task has repeatedly ran
        Thread.sleep(1100);
        int runCount = task.getRunCount();
        assertTrue(runCount >= 10);
    }

    @Test
    public void testStop() {
        String id = taskManager.scheduleTask(new TestTask(), TASK_DELAY);
        taskManager.stopTask(id, null, null);
        assertEquals(STOPPED, taskManager.getTaskState(id).getStatus());
    }

    @Test
    public void testPause() {
        String id = taskManager.scheduleTask(new TestTask(), TASK_DELAY);
        taskManager.pauseTask(id, null, null);
        assertEquals(PAUSED, taskManager.getTaskState(id).getStatus());
    }

    @Test
    public void testResume() {
        String id = taskManager.scheduleTask(new TestTask(), TASK_DELAY);
        taskManager.pauseTask(id, null, null);
        assertEquals(PAUSED, taskManager.getTaskState(id).getStatus());
        taskManager.resumeTask(id, null, null);
        assertEquals(SCHEDULED, taskManager.getTaskState(id).getStatus());
    }


    // There is a concurrency issue with this test that I currently cannot fix
    @Ignore
    public void testRestart() {
        String id = taskManager.scheduleTask(new TestTask(), 0);
        taskManager.stopTask(id, null, null);
        assertEquals(STOPPED, taskManager.getTaskState(id).getStatus());

        taskManager.restartTask(id, null, null);
        assertNotEquals(STOPPED, taskManager.getTaskState(id).getStatus());
    }

    // There is a concurrency issue with this test that I currently cannot fix
    @Ignore
    public void testTaskStateRaceCondition() {
        for (int i = 0; i < 100000 ; i++) {
            String id = taskManager.scheduleTask(new TestTask(), 0);
            taskManager.stopTask(id, null, null);
            assertEquals( STOPPED, taskManager.getTaskState(id).getStatus());

            taskManager.restartTask(id, null, null);
            assertNotEquals(STOPPED, taskManager.getTaskState(id).getStatus());
        }
    }

    @Test
    public void testGetAll() {
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            ids.add(taskManager.scheduleTask(new TestTask(), TASK_DELAY));
        }

        // taskManager can now contain completed tasks from other tests
        Set<String> allTasks = taskManager.getAllTasks();
        ids.forEach(x -> assertTrue(allTasks.contains(x)));
    }

    @Test
    public void testGetTasks() {
        Set<String> paused = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            String id = taskManager.scheduleTask(new TestTask(), TASK_DELAY);
            if(i%2 == 0) {
                taskManager.pauseTask(id, null, null);
                paused.add(id);
            }
        }

        assertEquals(paused.size(), taskManager.getTasks(PAUSED).size());
        taskManager.getTasks(PAUSED).forEach(x -> assertTrue(paused.contains(x)));
    }
}
