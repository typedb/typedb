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

package ai.grakn.engine.controller;

import ai.grakn.engine.backgroundtasks.*;
import com.jayway.restassured.http.ContentType;
import ai.grakn.engine.GraknEngineTestBase;
import com.jayway.restassured.response.Response;
import org.hamcrest.Matchers;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;

import static ai.grakn.engine.backgroundtasks.TaskStatus.COMPLETED;
import static ai.grakn.engine.backgroundtasks.TaskStatus.STOPPED;
import static com.jayway.restassured.RestAssured.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;

public class TaskControllerTest extends GraknEngineTestBase {
    private TaskManager taskManager;
    private String singleTask;

    @Before
    public void setUp() throws Exception {
        taskManager = InMemoryTaskManager.getInstance();
        singleTask = taskManager.scheduleTask(new TestTask(), this.getClass().getName(), new Date(), 0, new JSONObject());
        taskManager.stopTask(singleTask, this.getClass().getName());
    }

    @Test
    public void testTasksByStatus() {
        System.out.println("testTasksByStatus");
        Response response = given().queryParam("status", COMPLETED.toString())
                                   .get("/tasks/all");

        response.then().statusCode(200)
                .and().contentType(ContentType.JSON)
                .and().body("list.size()", greaterThanOrEqualTo(1));

        JSONArray array = new JSONArray(response.body().asString());
        array.forEach(x -> {
            JSONObject o = (JSONObject)x;
            assertEquals(COMPLETED.toString(), o.get("status"));
        });
    }

    @Test
    public void testTasksByClassName() {
        System.out.println("testTasksByClassName");
        Response response = given().queryParam("className", TestTask.class.getName())
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
        System.out.println("testTasksByCreator");
       Response response = given().queryParam("creator", this.getClass().getName())
                                   .get("/tasks/all");

        response.then().statusCode(200)
                .and().contentType(ContentType.JSON)
                .and().body("list.size()", greaterThanOrEqualTo(1));

        JSONArray array = new JSONArray(response.body().asString());
        array.forEach(x -> {
            JSONObject o = (JSONObject)x;
            assertEquals(this.getClass().getName(), o.get("creator"));
        });
    }

    @Test
    public void testGetAllTasks() {
        System.out.println("testGetAllTasks");
        taskManager.storage().clear();

        singleTask = taskManager.scheduleTask(new TestTask(), this.getClass().getName(), new Date(), 0, new JSONObject());
        taskManager.stopTask(singleTask, this.getClass().getName());
        get("/tasks/all")
                .then().statusCode(200)
                .and().contentType(ContentType.JSON)
                .and().body("list.size()", greaterThanOrEqualTo(1));
    }

    @Test
    public void testGetTask() throws Exception {
        System.out.println("testGetTask");
        get("/tasks/"+singleTask)
                .then().statusCode(200)
                .and().contentType(ContentType.JSON)
                .and().body("id", equalTo(singleTask))
                .and().body("status", equalTo(STOPPED.toString()));
    }

    @Test
    public void testScheduleWithoutOptional() {
        System.out.println("testScheduleWithoutOptional");
        given().queryParam("className", TestTask.class.getName())
               .queryParam("creator", this.getClass().getName())
               .queryParam("runAt", new Date())
               .post("/tasks/schedule")
               .then().statusCode(200)
               .and().contentType(ContentType.JSON)
               .and().body("id", notNullValue());
    }

    @Test
    public void testScheduleStopTask() {
        System.out.println("testScheduleStopTask");
        Response response = given().queryParam("className", LongRunningTask.class.getName())
                .queryParam("creator", this.getClass().getName())
                .queryParam("runAt", new Date())
                .queryParam("interval", 5000)
                .post("/tasks/schedule");

        response.then().statusCode(200)
                .and().contentType(ContentType.JSON);

        String id = new JSONObject(response.body().asString()).getString("id");
        System.out.println(id);

        // Stop task
        put("/tasks/"+id+"/stop")
                .then().statusCode(200)
                .and().contentType("text/html");

        // Check state
        get("/tasks/"+id)
                .then().statusCode(200)
                .and().contentType(ContentType.JSON)
                .and().body("id", equalTo(id))
                .and().body("status", equalTo(STOPPED.toString()));
    }
}
