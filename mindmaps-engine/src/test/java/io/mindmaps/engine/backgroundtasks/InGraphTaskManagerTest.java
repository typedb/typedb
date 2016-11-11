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
import io.mindmaps.exception.MindmapsValidationException;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class InGraphTaskManagerTest extends MindmapsEngineTestBase {
    private InGraphTaskManager taskManager;

    @Before
    public void setUp() {
        taskManager = new InGraphTaskManager();
    }

    @Test
    public void testSaveNewState() throws Exception {
        TaskState state = new TaskState("test state");
        state.setStatus(TaskStatus.CREATED)
                .setStatusChangedBy(this.getClass().getName())
                .setStatusChangeMessage("testing")
                .setCreator(this.getClass().getName())
                .setDelay(100)
                .setInterval(200)
                .setQueuedTime(new Date())
                .setRecurring(false);

        String id = taskManager.saveNewState(state);
        assertNotNull(id);

        // Sleep to make sure Dates() don't match if we are not deserialising properly.
        Thread.sleep(1000);

        TaskState ret = taskManager.getTaskState(id);
        assertNotNull(ret);

        assertEquals("delay", state.getDelay(), ret.getDelay());
        assertEquals("interval", state.getInterval(), ret.getInterval());
        assertEquals("status change time", state.getStatusChangeTime().toString(), ret.getStatusChangeTime().toString());
        assertEquals("status change message", state.getStatusChangeMessage(), ret.getStatusChangeMessage());
        assertEquals("status changed by", state.getStatusChangedBy(), ret.getStatusChangedBy());
        // Queued time is set by TaskManager.
        assertEquals("creator", state.getCreator(), ret.getCreator());
        assertEquals("state task status", state.getStatus(), ret.getStatus());
        // Executing hostname is set by TaskManager.
        assertEquals("recurring", state.getRecurring(), ret.getRecurring());
    }

    @Test
    public void testGetAllTasks() throws MindmapsValidationException {
        Set<String> savedIDs = new HashSet<>();

        // Generate some fake tasks
        for (int i = 0; i < 10; i++) {
            TaskState state = new TaskState("testGetAllTasks "+String.valueOf(i))
                    .setStatusChangeMessage("test message "+String.valueOf(i))
                    .setDelay(i*1000)
                    .setInterval(i*1001)
                    .setRecurring(i%2 == 0);

            savedIDs.add(taskManager.saveNewState(state));
        }

        Set<String> returnedIDs = taskManager.getAllTasks();

        // getAllTasks can return states added by other @Test methods
        assertTrue(returnedIDs.size() >= savedIDs.size());
        savedIDs.forEach(x -> assertTrue(returnedIDs.contains(x)));
    }

    @Test
    public void testGetTasks() throws MindmapsValidationException {
        Set<String> scheduledIDs = new HashSet<>();

        for (int i = 0; i < 10; i++) {
            TaskState state = new TaskState("testGetTasks "+String.valueOf(i));

            if(i%2 == 0) {
                state.setStatus(TaskStatus.SCHEDULED);
                scheduledIDs.add(taskManager.saveNewState(state));
            } else {
                taskManager.saveNewState(state);
            }
        }

        Set<String> returnedIDs = taskManager.getTasks(TaskStatus.SCHEDULED);

        assertTrue(returnedIDs.size() >= scheduledIDs.size());
        scheduledIDs.forEach(x -> assertTrue(returnedIDs.contains(x)));
    }

    @Test
    public void testUpdateTaskState() throws MindmapsValidationException{
        TaskState state = new TaskState("testUpdateTaskState");
        state.setStatus(TaskStatus.CREATED)
                .setStatusChangedBy(this.getClass().getName())
                .setStatusChangeMessage("testing")
                .setCreator(this.getClass().getName())
                .setDelay(100)
                .setInterval(200)
                .setQueuedTime(new Date())
                .setRecurring(true);

        String id = taskManager.saveNewState(state);
        assertNotNull(id);

        // Update state
        TaskState newState = taskManager.getTaskState(id);
        newState.setStatus(TaskStatus.SCHEDULED)
                .setStatusChangeMessage("mutating")
                .setDelay(123)
                .setInterval(43444)
                .setRecurring(false);

        String newID = taskManager.updateTaskState(id, newState);

        // The id should not have changed
        assertEquals(id, newID);

        // Retrieve updated state
        TaskState updated = taskManager.getTaskState(newID);

        // Check that updated field differ, whilst others match
        assertNotEquals("task status", state.getStatus(), updated.getStatus());
        assertEquals("status changed by", state.getStatusChangedBy(), updated.getStatusChangedBy());
        assertNotEquals("status change message", state.getStatusChangeMessage(), updated.getStatusChangeMessage());
        assertEquals("creator", state.getCreator(), updated.getCreator());
        assertNotEquals("delay", state.getDelay(), updated.getDelay());
        assertNotEquals("interval", state.getInterval(), updated.getInterval());
        assertEquals("queued time", state.getQueuedTime(), updated.getQueuedTime());
        assertNotEquals("recurring", state.getRecurring(), updated.getRecurring());
    }
}
