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

package ai.grakn.test.engine.controller;

import ai.grakn.engine.backgroundtasks.standalone.StandaloneTaskManager;
import ai.grakn.engine.backgroundtasks.TaskManager;
import ai.grakn.test.AbstractEngineTest;
import ai.grakn.test.engine.backgroundtasks.LongRunningTask;
import ai.grakn.test.engine.backgroundtasks.TestTask;
import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.response.Response;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;

import static ai.grakn.engine.backgroundtasks.TaskStatus.STOPPED;
import static ai.grakn.util.REST.WebPath.TASKS_SCHEDULE_URI;
import static com.jayway.restassured.RestAssured.get;
import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.RestAssured.put;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;

public class TasksControllerTest extends AbstractEngineTest {
    private TaskManager taskManager;
    private String singleTask;

    @Before
    public void setUp() throws Exception {
        assumeFalse(usingTinker());
        taskManager = StandaloneTaskManager.getInstance();
        singleTask = taskManager.scheduleTask(new LongRunningTask(), this.getClass().getName(), new Date(), 0, new JSONObject());
        taskManager.stopTask(singleTask, this.getClass().getName());
    }

    @Test
    public void testTasksByStatus() {
        Response response = given().queryParam("status", STOPPED.toString())
                                   .queryParam("limit", 10)
                                   .get("/tasks/all");

        response.then().statusCode(200)
                .and().contentType(ContentType.JSON)
                .and().body("list.size()", greaterThanOrEqualTo(1));

        JSONArray array = new JSONArray(response.body().asString());
        array.forEach(x -> {
            JSONObject o = (JSONObject)x;
            assertEquals(STOPPED.toString(), o.get("status"));
        });
    }

    @Test
    public void testTasksByClassName() {
        System.out.println("testTasksByClassName");
        Response response = given().queryParam("className", TestTask.class.getName())
                                   .queryParam("limit", 10)
                                   .get("/tasks/all");

        response.then().statusCode(200)
                .and().contentType(ContentType.JSON)
                .and().body("list.size()", greaterThanOrEqualTo(1));

        JSONArray array = new JSONArray(response.body().asString());
        array.forEach(x -> {
            JSONObject o = (JSONObject)x;
            assertEquals(TestTask.class.getName(), o.get("className"));
        });
    }

    @Test
    public void testTasksByCreator() {
        Response response = given().queryParam("creator", this.getClass().getName())
                                   .queryParam("limit", 10)
                                   .get("/tasks/all");

        response.then().statusCode(200)
                .and().contentType(ContentType.JSON)
                .and().body("list.size()", greaterThanOrEqualTo(1));

        JSONArray array = new JSONArray(response.body().asString());
        array.forEach(x -> {
            JSONObject o = (JSONObject)x;
            Assert.assertEquals(this.getClass().getName(), o.get("creator"));
        });
    }

    @Test
    public void testGetAllTasks() {
        Response response = given().queryParam("limit", 10).get("/tasks/all");

        System.out.println(response.body().asString());

        response.then().statusCode(200)
                .and().contentType(ContentType.JSON)
                .and().body("list.size()", greaterThanOrEqualTo(1));
    }

    @Test
    public void testGetTask() throws Exception {
        get("/tasks/"+singleTask)
                .then().statusCode(200)
                .and().contentType(ContentType.JSON)
                .and().body("id", equalTo(singleTask))
                .and().body("status", equalTo(STOPPED.toString()));
    }

    @Test
    public void testScheduleWithoutOptional() {
        given().queryParam("className", TestTask.class.getName())
               .queryParam("creator", this.getClass().getName())
               .queryParam("runAt", new Date())
               .post(TASKS_SCHEDULE_URI)
               .then().statusCode(200)
               .and().contentType(ContentType.JSON)
               .and().body("id", notNullValue());
    }

    @Test
    public void testScheduleStopTask() {
        Response response = given()
                .queryParam("className", LongRunningTask.class.getName())
                .queryParam("creator", this.getClass().getName())
                .queryParam("runAt", new Date())
                .queryParam("interval", 5000)
                .post(TASKS_SCHEDULE_URI);

        System.out.println(response.body().asString());

        response.then().statusCode(200)
                .and().contentType(ContentType.JSON);

        String id = new JSONObject(response.body().asString()).getString("id");
        System.out.println(id);

        // Stop task
        put("/tasks/"+id+"/stop")
                .then().statusCode(200)
                .and().contentType("text/html");

        // Check taskstorage
        get("/tasks/"+id)
                .then().statusCode(200)
                .and().contentType(ContentType.JSON)
                .and().body("id", equalTo(id))
                .and().body("status", equalTo(STOPPED.toString()));
    }
}
