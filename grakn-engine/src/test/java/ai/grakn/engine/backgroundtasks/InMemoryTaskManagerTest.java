/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs
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

import ai.grakn.engine.MindmapsEngineTestBase;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static ai.grakn.engine.backgroundtasks.TaskStatus.PAUSED;
import static ai.grakn.engine.backgroundtasks.TaskStatus.RUNNING;
import static ai.grakn.engine.backgroundtasks.TaskStatus.STOPPED;
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

        UUID uuid = taskManager.scheduleTask(task, 0);
        assertNotEquals(TaskStatus.CREATED, taskManager.getTaskState(uuid).getStatus());

        // Wait for supervisor thread to mark task as completed
        Thread.sleep(2000);

        // Check that task has ran
        assertEquals(TaskStatus.COMPLETED, taskManager.getTaskState(uuid).getStatus());
        assertTrue(task.getRunCount() > runCount);
    }

    @Test
    public void testRecurring() throws Exception {
        TestTask task = new TestTask();
        UUID uuid = taskManager.scheduleRecurringTask(task, 100, 100);
        assertNotEquals(TaskStatus.CREATED, taskManager.getTaskState(uuid).getStatus());

        // Check that task has repeatedly ran
        Thread.sleep(1100);
        int runCount = task.getRunCount();
        System.out.println("task run count: "+Integer.toString(runCount));
        assertTrue(runCount >= 10);
    }

    @Test
    public void testStop() {
        UUID uuid = taskManager.scheduleTask(new TestTask(), TASK_DELAY);
        taskManager.stopTask(uuid);
        assertEquals(STOPPED, taskManager.getTaskState(uuid).getStatus());
    }

    @Test
    public void testPause() {
        UUID uuid = taskManager.scheduleTask(new TestTask(), TASK_DELAY);
        taskManager.pauseTask(uuid);
        assertEquals(PAUSED, taskManager.getTaskState(uuid).getStatus());
    }

    @Test
    public void testResume() {
        UUID uuid = taskManager.scheduleTask(new TestTask(), TASK_DELAY);
        taskManager.pauseTask(uuid);
        assertEquals(PAUSED, taskManager.getTaskState(uuid).getStatus());
        taskManager.resumeTask(uuid);
        assertEquals(RUNNING, taskManager.getTaskState(uuid).getStatus());
    }

    @Test
    public void testRestart() {
        UUID uuid = taskManager.scheduleTask(new TestTask(), 0);
        taskManager.stopTask(uuid);
        assertEquals(STOPPED, taskManager.getTaskState(uuid).getStatus());

        taskManager.restartTask(uuid);
        assertNotEquals(STOPPED, taskManager.getTaskState(uuid).getStatus());
    }

    @Test
    public void testGetAll() {
        Set<UUID> uuids = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            uuids.add(taskManager.scheduleTask(new TestTask(), TASK_DELAY));
        }

        // taskManager can now contain completed tasks from other tests
        Set<UUID> allTasks = taskManager.getAllTasks();
        uuids.forEach(x -> assertTrue(allTasks.contains(x)));
    }

    @Test
    public void testGetTasks() {
        Set<UUID> paused = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            UUID uuid = taskManager.scheduleTask(new TestTask(), TASK_DELAY);
            if(i%2 == 0) {
                taskManager.pauseTask(uuid);
                paused.add(uuid);
            }
        }

        assertEquals(paused.size(), taskManager.getTasks(PAUSED).size());
        taskManager.getTasks(PAUSED).forEach(x -> assertTrue(paused.contains(x)));
    }
}
