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

import io.mindmaps.engine.backgroundtasks.types.BackgroundTask;
import io.mindmaps.engine.backgroundtasks.types.TaskStatus;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class InMemoryTaskRunnerTest {
    private InMemoryTaskRunner taskRunner;
    private static String TASK_NAME = "Empty BackgroundTask() for unit testing";
    private static long TASK_DELAY = 100000;

    @Before
    public void setUp() {
        taskRunner = InMemoryTaskRunner.getInstance();
    }

    @Test
    public void testQueueAndRetrieve() throws Exception {
        UUID uuid = taskRunner.scheduleTask(new BackgroundTask(TASK_NAME), 0);
        assertNotEquals(TaskStatus.CREATED, taskRunner.taskStatus(uuid));

        // Check that task has ran
        Thread.sleep(1000);
        assertEquals(TaskStatus.COMPLETED, taskRunner.taskStatus(uuid));
    }

    @Test
    public void testStop() {
        UUID uuid = taskRunner.scheduleTask(new BackgroundTask(TASK_NAME), TASK_DELAY);
        taskRunner.stopTask(uuid);
        assertEquals(TaskStatus.STOPPED, taskRunner.taskStatus(uuid));
    }

    @Test
    public void testPause() {
        UUID uuid = taskRunner.scheduleTask(new BackgroundTask(TASK_NAME), TASK_DELAY);
        taskRunner.pauseTask(uuid);
        assertEquals(TaskStatus.PAUSED, taskRunner.taskStatus(uuid));
    }

    @Test
    public void testResume() {
        UUID uuid = taskRunner.scheduleTask(new BackgroundTask(TASK_NAME), TASK_DELAY);
        taskRunner.pauseTask(uuid);
        assertEquals(TaskStatus.PAUSED, taskRunner.taskStatus(uuid));
        taskRunner.resumeTask(uuid);
        assertEquals(TaskStatus.RUNNING, taskRunner.taskStatus(uuid));
    }

    @Test
    public void testGetAll() {
        Set<UUID> uuids = new HashSet<>();

        for (int i = 0; i < 10; i++) {
            uuids.add(taskRunner.scheduleTask(new BackgroundTask(TASK_NAME+Integer.toString(i)), TASK_DELAY));
        }

        // taskRunner can now contain completed tasks from other tests
        Set<UUID> allTasks = taskRunner.getAllTasks();
        uuids.forEach(x -> assertTrue(allTasks.contains(x)));
    }

    @Test
    public void testGetTasks() {
        Set<UUID> paused = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            UUID uuid = taskRunner.scheduleTask(new BackgroundTask(TASK_NAME+Integer.toString(i)), TASK_DELAY);
            if(i%2 == 0) {
                taskRunner.pauseTask(uuid);
                paused.add(uuid);
            }
        }

        assertEquals(paused.size(), taskRunner.getTasks(TaskStatus.PAUSED).size());
        taskRunner.getTasks(TaskStatus.PAUSED).forEach(x -> assertTrue(paused.contains(x)));
    }
}
