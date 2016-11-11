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

package ai.grakn.engine.controller;

import ai.grakn.engine.backgroundtasks.TaskManager;
import com.jayway.restassured.http.ContentType;
import ai.grakn.engine.GraknEngineTestBase;
import ai.grakn.engine.backgroundtasks.InMemoryTaskManager;
import ai.grakn.engine.backgroundtasks.TestTask;
import org.junit.Before;
import org.junit.Test;

import static ai.grakn.engine.backgroundtasks.TaskStatus.COMPLETED;
import static ai.grakn.engine.backgroundtasks.TaskStatus.PAUSED;
import static ai.grakn.engine.backgroundtasks.TaskStatus.RUNNING;
import static ai.grakn.engine.backgroundtasks.TaskStatus.SCHEDULED;
import static ai.grakn.engine.backgroundtasks.TaskStatus.STOPPED;
import static com.jayway.restassured.RestAssured.put;
import static com.jayway.restassured.RestAssured.get;
import static org.hamcrest.Matchers.*;

public class BackgroundTaskControllerTest extends GraknEngineTestBase {
    private TaskManager taskManager;
    private String singleTask;

    @Before
    public void setUp() throws Exception {
        taskManager = InMemoryTaskManager.getInstance();
        singleTask = taskManager.scheduleTask(new TestTask(), 0).toString();

        // Wait for task to finish
        Thread.sleep(1000);
    }

    @Test
    public void testGetAllTasks() throws Exception {
        get("/backgroundtasks/all")
                .then().statusCode(200)
                .and().contentType(ContentType.JSON)
                .and().body("keySet()", hasItem(singleTask))
                .and().body(singleTask, equalTo(COMPLETED.toString()));
    }

    @Test
    public void testTasksByStatus() throws Exception {
        get("/backgroundtasks/tasks/"+ COMPLETED.toString())
                .then().statusCode(200)
                .and().contentType(ContentType.JSON)
                .and().body("size()", greaterThanOrEqualTo(1))
                .and().extract().jsonPath().getList("$").contains(singleTask);
    }

    @Test
    public void testGetTask() {
        get("/backgroundtasks/task/"+singleTask)
                .then().statusCode(200)
                .and().body("status", equalTo(COMPLETED.toString()));
    }

    @Test
    public void testPauseResume() {
        String uuid = taskManager.scheduleTask(new TestTask(), 1000).toString();

        // Pause task.
        put("/backgroundtasks/task/"+uuid+"/pause").then().statusCode(200);

        // Check task status.
        get("/backgroundtasks/task/"+uuid)
                .then().statusCode(200)
                .and().contentType(ContentType.JSON)
                .and().body("status", equalTo(PAUSED.toString()));

        // Resume task.
        put("/backgroundtasks/task/"+uuid+"/resume").then().statusCode(200);

        // Check task status.
        get("/backgroundtasks/task/"+uuid)
                .then().statusCode(200)
                .and().contentType(ContentType.JSON)
                .and().body("status", anyOf(equalTo(RUNNING.toString()),
                                                  equalTo(COMPLETED.toString())));
    }

    @Test
    public void testStopRestart() {
        String uuid = taskManager.scheduleTask(new TestTask(), 1000).toString();

        // Stop task.
        put("/backgroundtasks/task/"+uuid+"/stop").then().statusCode(200);

        // Check task is stopped.
        get("/backgroundtasks/task/"+uuid)
                .then().statusCode(200)
                .and().contentType(ContentType.JSON)
                .and().body("status", equalTo(STOPPED.toString()));

        // Restart task.
        put("/backgroundtasks/task/"+uuid+"/restart").then().statusCode(200);

        get("/backgroundtasks/task/"+uuid)
                .then().statusCode(200)
                .and().contentType(ContentType.JSON)
                .and().body("status", anyOf(equalTo(SCHEDULED.toString()),
                                                  equalTo(RUNNING.toString()),
                                                  equalTo(COMPLETED.toString())));
    }
}
