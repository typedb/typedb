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
 *
 */

package ai.grakn.engine.controller;

import ai.grakn.engine.postprocessing.PostProcessor;
import ai.grakn.engine.tasks.manager.TaskManager;
import com.jayway.restassured.http.ContentType;
import mjson.Json;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import static com.jayway.restassured.RestAssured.given;
import static org.apache.http.HttpStatus.SC_OK;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Felix Chapman
 */
public class CommitLogControllerTest {

    private static final String CONCEPTS_TO_FIX = "hello";
    private static final String TYPES_WITH_NEW_COUNTS = "yes ok";
    private static final String BODY =
            Json.object("concepts-to-fix", CONCEPTS_TO_FIX, "types-with-new-counts", TYPES_WITH_NEW_COUNTS).toString();

    private static final int DELAY = 100;
    private final TaskManager taskManager = mock(TaskManager.class);
    private final PostProcessor postProcessor = mock(PostProcessor.class);

    @Rule
    public final SparkContext sparkContext = SparkContext.withControllers(spark -> {
        new CommitLogController(spark, DELAY, taskManager, postProcessor);
    });

    @Test
    public void whenPostingToCommitLogEndpoint_Return200() {
        given().body(BODY).when().post("/kb/myks/commit_log").then().statusCode(SC_OK);
    }

    @Test
    public void whenPostingToCommitLogEndpoint_ReturnJSON() {
        given().body(BODY).when().post("/kb/myks/commit_log").then().contentType(ContentType.JSON);
    }

    @Test
    public void whenPostingToCommitLogEndpoint_ReceiveBodyWithTasks() {
        given().body(BODY).when().post("/kb/myks/commit_log").then()
                .body("postProcessingTaskId", isA(String.class));
    }

    @Test
    public void whenPostingToCommitLogEndpoint_ReceiveBodyWithKeyspace() {
        given().body(BODY).when().post("/kb/myks/commit_log").then().body("keyspace", is("myks"));
    }

    @Test
    public void whenPostingToCommitLogEndpoint_SubmitTwoTasks() {
        given().body(BODY).post("/kb/myks/commit_log");

        verify(taskManager, Mockito.times(1)).addTask(any(), any());
    }
}