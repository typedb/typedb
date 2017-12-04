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

import ai.grakn.Keyspace;
import ai.grakn.engine.postprocessing.PostProcessingTask;
import ai.grakn.engine.postprocessing.PostProcessor;
import ai.grakn.engine.tasks.manager.TaskConfiguration;
import ai.grakn.engine.tasks.manager.TaskManager;
import ai.grakn.engine.tasks.manager.TaskState;
import ai.grakn.kb.log.CommitLog;
import ai.grakn.util.REST;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import spark.Request;
import spark.Response;
import spark.Service;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static ai.grakn.engine.controller.util.Requests.mandatoryPathParameter;
import static ai.grakn.util.REST.Request.COMMIT_LOG_COUNTING;
import static ai.grakn.util.REST.Request.COMMIT_LOG_FIXING;

/**
 * A controller which core submits commit logs to so we can post-process jobs for cleanup.
 *
 * @author Filipe Peliz Pinto Teixeira
 */
public class CommitLogController {
    private static final ObjectMapper mapper = new ObjectMapper();
    private final TaskManager manager;
    private final PostProcessor postProcessor;

    public CommitLogController(Service spark, TaskManager manager, PostProcessor postProcessor){
        this.manager = manager;
        this.postProcessor = postProcessor;

        spark.post(REST.WebPath.COMMIT_LOG_URI, this::submitConcepts);
    }

    @POST
    @Path("/kb/{keyspace}/commit_log")
    @ApiOperation(value = "Submits post processing jobs for a specific keyspace")
    @ApiImplicitParams({
        @ApiImplicitParam(name = REST.Request.KEYSPACE_PARAM, value = "The key space of an opened graph", required = true, dataType = "string", paramType = "path"),
        @ApiImplicitParam(name = COMMIT_LOG_FIXING, value = "A Json Array of IDs representing concepts to be post processed", required = true, dataType = "string", paramType = "body"),
        @ApiImplicitParam(name = COMMIT_LOG_COUNTING, value = "A Json Array types with new and removed instances", required = true, dataType = "string", paramType = "body")
    })
    private String submitConcepts(Request req, Response res) throws IOException {
        Keyspace keyspace = Keyspace.of(mandatoryPathParameter(req, REST.Request.KEYSPACE_PARAM));

        //TODO: Is this really necessary? Will it add that much overhead?
        //Seperate commit logs are needed to prevent logging more info than needed.
        // For example PP does not need to know the instance count
        CommitLog commitLogUpdateCount = mapper.readValue(req.body(), CommitLog.class);
        CommitLog commitLogPP = mapper.readValue(req.body(), CommitLog.class);
        commitLogPP.instanceCount().clear();

        // Things to post process
        TaskState postProcessingTaskState = PostProcessingTask.createTask(this.getClass());

        // TODO Use an engine wide executor here
        CompletableFuture.allOf(
                CompletableFuture.runAsync(() -> postProcessor.updateCounts(keyspace, commitLogUpdateCount)),
                CompletableFuture.runAsync(() -> manager.addTask(postProcessingTaskState, TaskConfiguration.of(commitLogPP))))
                .join();

        return "";
    }
}
