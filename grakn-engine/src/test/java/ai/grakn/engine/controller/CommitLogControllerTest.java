/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

import ai.grakn.Keyspace;
import ai.grakn.engine.postprocessing.PostProcessor;
import ai.grakn.engine.tasks.manager.TaskManager;
import ai.grakn.kb.log.CommitLog;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

import static com.jayway.restassured.RestAssured.given;
import static org.apache.http.HttpStatus.SC_OK;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Felix Chapman
 */
public class CommitLogControllerTest {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Keyspace keyspace = Keyspace.of("myks");
    private static final CommitLog commitLog = CommitLog.create(keyspace, Collections.emptyMap(), Collections.emptyMap());

    private final TaskManager taskManager = mock(TaskManager.class);
    private final PostProcessor postProcessor = mock(PostProcessor.class);

    @Rule
    public final SparkContext sparkContext = SparkContext.withControllers(spark -> {
        new CommitLogController(spark, taskManager, postProcessor);
    });

    @Test
    public void whenPostingToCommitLogEndpoint_Return200() throws JsonProcessingException {
        given().body(mapper.writeValueAsString(commitLog)).when().post("/kb/" + keyspace.getValue() +"/commit_log").then().statusCode(SC_OK);
    }

    @Test
    public void whenPostingToCommitLogEndpoint_SubmitTwoTasks() throws JsonProcessingException {
        given().body(mapper.writeValueAsString(commitLog)).post("/kb/" + keyspace.getValue() +"/commit_log");

        verify(taskManager, Mockito.times(1)).addTask(any(), any());
    }
}