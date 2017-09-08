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
import ai.grakn.engine.postprocessing.UpdatingInstanceCountTask;
import ai.grakn.engine.tasks.manager.TaskConfiguration;
import ai.grakn.engine.tasks.manager.TaskManager;
import ai.grakn.engine.tasks.manager.TaskState;
import ai.grakn.util.REST;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import spark.Request;
import spark.Response;
import spark.Service;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static ai.grakn.util.REST.Request.COMMIT_LOG_COUNTING;
import static ai.grakn.util.REST.Request.COMMIT_LOG_FIXING;
import static ai.grakn.util.REST.Request.KEYSPACE_PARAM;

/**
 * A controller which core submits commit logs to so we can post-process jobs for cleanup.
 *
 * @author Filipe Teixeira
 */
//TODO Implement delete
public class CommitLogController {
    private final String defaultKeyspace;
    private final TaskManager manager;
    private final int postProcessingDelay;

    public CommitLogController(Service spark, String defaultKeyspace, int postProcessingDelay, TaskManager manager){
        this.defaultKeyspace = defaultKeyspace;
        this.postProcessingDelay = postProcessingDelay;
        this.manager = manager;

        spark.post(REST.WebPath.COMMIT_LOG_URI, this::submitConcepts);
        spark.delete(REST.WebPath.COMMIT_LOG_URI, this::deleteConcepts);
    }


    @DELETE
    @Path("/commit_log")
    @ApiOperation(value = "Delete all the post processing jobs for a specific keyspace")
    @ApiImplicitParam(name = "keyspace", value = "The key space of an opened graph", required = true, dataType = "string", paramType = "path")
    private String deleteConcepts(Request req, Response res){
        return "Delete not implemented";
    }


    @GET
    @Path("/commit_log")
    @ApiOperation(value = "Submits post processing jobs for a specific keyspace")
    @ApiImplicitParams({
        @ApiImplicitParam(name = "keyspace", value = "The key space of an opened graph", required = true, dataType = "string", paramType = "path"),
        @ApiImplicitParam(name = COMMIT_LOG_FIXING, value = "A Json Array of IDs representing concepts to be post processed", required = true, dataType = "string", paramType = "body"),
        @ApiImplicitParam(name = COMMIT_LOG_COUNTING, value = "A Json Array types with new and removed instances", required = true, dataType = "string", paramType = "body")
    })
    private String submitConcepts(Request req, Response res) {
        Keyspace keyspace = Keyspace.of(Optional.ofNullable(req.queryParams(KEYSPACE_PARAM)).orElse(defaultKeyspace));

        // Instances to post process
        TaskState postProcessingTaskState = PostProcessingTask.createTask(this.getClass(), postProcessingDelay);
        TaskConfiguration postProcessingTaskConfiguration = PostProcessingTask.createConfig(keyspace, req.body());

        //Instances to count
        TaskState countingTaskState = UpdatingInstanceCountTask.createTask(this.getClass());
        TaskConfiguration countingTaskConfiguration = UpdatingInstanceCountTask.createConfig(keyspace, req.body());

        // TODO Use an engine wide executor here
        CompletableFuture.allOf(
                CompletableFuture.runAsync(() -> manager.addTask(postProcessingTaskState, postProcessingTaskConfiguration)),
                CompletableFuture.runAsync(() -> manager.addTask(countingTaskState, countingTaskConfiguration)))
                .join();

        // TODO return Json
        return "PP Task [ " + postProcessingTaskState.getId().getValue() + " ] and Counting task [" + countingTaskState.getId().getValue() + "] created for graph [" + keyspace + "]";
    }
}
