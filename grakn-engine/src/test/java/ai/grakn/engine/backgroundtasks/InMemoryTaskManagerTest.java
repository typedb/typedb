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

package ai.grakn.engine.backgroundtasks;

import ai.grakn.engine.GraknEngineTestBase;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static ai.grakn.engine.backgroundtasks.TaskStatus.*;
import static org.junit.Assert.*;

public class InMemoryTaskManagerTest extends GraknEngineTestBase {
    private TaskManager taskManager;

    @Before
    public void setUp() {
        taskManager = InMemoryTaskManager.getInstance();
    }

    @Test
    public void testRunSingle() {
        TestTask task = new TestTask();

        String id = taskManager.scheduleTask(task, this.getClass().getName(), new Date(), 0, new JSONObject());

        // Wait for task to be executed.
        waitToFinish(id);

        assertEquals(COMPLETED, taskManager.storage().getState(id).status());
    }

    private void waitToFinish(String id) {
       StateStorage storage = taskManager.storage();
        final long initial = new Date().getTime();

        while ((new Date().getTime())-initial < 10000) {
            if (storage.getState(id).status() == COMPLETED)
                break;

            System.out.println("created: "+storage.getTasks(CREATED, null, null, 0, 0).size());
            System.out.println("scheduled: "+storage.getTasks(SCHEDULED, null, null,0, 0).size());
            System.out.println("completed: "+storage.getTasks(COMPLETED, null, null,0, 0).size());
            System.out.println("running: "+storage.getTasks(RUNNING, null, null,0, 0).size());

            try {
                Thread.sleep(100);
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void consecutiveRunSingle() {
        // Schedule tasks
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 100000; i++) {
            ids.add(taskManager.scheduleTask(new TestTask(), this.getClass().getName(), new Date(), 0, new JSONObject()));
        }

        // Check that they all finished
        for(String id: ids) {
            if(taskManager.storage().getState(id).status() != COMPLETED)
                waitToFinish(id);
            assertEquals(COMPLETED, taskManager.storage().getState(id).status());
        }
    }

    @Test
    public void concurrentConsecutiveRuns() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        executorService.submit(this::consecutiveRunSingle);
        executorService.submit(this::consecutiveRunSingle);
        executorService.submit(this::consecutiveRunSingle);

        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
    }

    @Test
    public void testRunRecurring() throws Exception {
        TestTask task = new TestTask();

        String id = taskManager.scheduleTask(task, this.getClass().getName(), new Date(), 100, new JSONObject());
        Thread.sleep(2000);

        assertTrue(task.getRunCount() > 1);

        // Stop task..
        taskManager.stopTask(id, null);
    }

    @Test
    public void testStopSingle() {
        BackgroundTask task = new LongRunningTask();
        String id = taskManager.scheduleTask(task, this.getClass().getName(), new Date(), 0, new JSONObject());

        TaskStatus status = taskManager.storage().getState(id).status();
        assertTrue(status == SCHEDULED || status == RUNNING);

        taskManager.stopTask(id, this.getClass().getName());

        status = taskManager.storage().getState(id).status();
        assertEquals(STOPPED, status);
    }

    @Test
    public void consecutiveStopStart() {
        for (int i = 0; i < 100000; i++) {
            testStopSingle();
        }
    }

    @Test
    public void concurrentConsecutiveStopStart() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.submit(this::consecutiveStopStart);
        executorService.submit(this::consecutiveStopStart);

        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
    }
}
