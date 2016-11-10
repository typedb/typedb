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

import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Concept;
import io.mindmaps.concept.Instance;
import io.mindmaps.engine.util.ConfigProperties;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.graql.Var;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static io.mindmaps.engine.backgroundtasks.TaskStatus.RUNNING;
import static io.mindmaps.graql.Graql.var;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class InGraphTaskManager extends AbstractTaskManager {
    private final static String SYSTEM_KEYSPACE = "grakn-system";
    private final Logger LOG = LoggerFactory.getLogger(InGraphTaskManager.class);
    private static ConfigProperties properties = ConfigProperties.getInstance();

    private MindmapsGraph graph;
    private Map<String, ScheduledFuture<BackgroundTask>> taskFutures;
    private ScheduledExecutorService executorService;

    private String hostname;

    public InGraphTaskManager() {
        graph = GraphFactory.getInstance().getGraph(SYSTEM_KEYSPACE);
        taskFutures = new ConcurrentHashMap<>();

        // Config sets to 0.0.0.0 which is fine only for single instance deployments.
        hostname = properties.getPath(ConfigProperties.SERVER_HOST_NAME);
    }

    /*
    TaskManager Required
     */
    public TaskState getTaskState(String id) {
        Instance instance = graph.getInstance(id);

        if(instance == null)
            return null;

        return new TaskState(getResourceValue(instance, "task-name"))
                .setDelay(Long.valueOf(getResourceValue(instance, "delay")))
                .setInterval(Long.valueOf(getResourceValue(instance, "interval")))
                .setStatusChangeMessage(getResourceValue(instance, "status-change-message"))
                .setStatusChangedBy(getResourceValue(instance, "status-change-by"))
                .setQueuedTime(new Date(Long.valueOf(getResourceValue(instance, "queued-time"))))
                .setCreator(getResourceValue(instance, "created-by"))
                .setStatus(TaskStatus.valueOf(getRelationValue("status-of-task",
                                                                id,
                                                               "status-of-task-value",
                                                               "task-status-value")))
                // Calling .setStatup updates statusChangeTime, to we override it afterwards.
                .setStatusChangeTime(new Date(Long.valueOf(getResourceValue(instance, "status-change-time"))))
                .setExecutingHostname(getRelationValue("task-executing-hostname",
                                                        id,
                                                        "task-executing-hostname-value",
                                                        "executing-hostname-value"))
                .setRecurring(isInRel("task-recurrence", id, "task-recurrence-value"));
    }

    public Set<String> getAllTasks() {
        return graph.graql().match(var("x").isa("scheduled-task"))
                            .get("x").map(Concept::getId)
                            .collect(Collectors.toSet());
    }

    public Set<String> getTasks(TaskStatus status) {
        return graph.graql().match(var().isa("status-of-task")
                                        .rel(var("x").isa("scheduled-task"))
                                        .rel("status-of-task-value", var().has("task-status-value", status.toString())))
                            .get("x").map(Concept::getId)
                            .collect(Collectors.toSet());
    }

    /*
    AbstractTaskManager required
     */
    protected String saveNewState(TaskState state) throws MindmapsValidationException {
        // Time when TaskState was written to the graph.
        state.setQueuedTime(new Date());

        // We will use this var later.
        Var serialised = serialiseTaskToVar(state);

        QueryBuilder queryBuilder = graph.graql();
        queryBuilder.insert(addRelations(serialised, state))
                    .execute();

        // Commit transaction
        graph.commit();

        return getVarID(serialised);
    }

    protected String updateTaskState(String id, TaskState state) throws MindmapsValidationException {
        // Delete all the old resources & relations of this state in the graph.
        graph.graql().match(var("x").rel(var().id(id)))
                     .delete("x")
                     .execute();

        Var serialised = serialiseTaskToVar(state);

        // We are updating an existing instance.
        serialised.id(id);
        graph.graql().insert(addRelations(serialised, state))
                     .execute();

        graph.commit();

        // Graph will be committed by saveToGraph.
        return getVarID(serialised);
    }

    protected ScheduledFuture<BackgroundTask> getTaskExecutionStatus(String id) {
        return taskFutures.get(id);
    }

    protected void executeSingle(String id, BackgroundTask task, long delay) {
        ScheduledFuture<BackgroundTask> f = (ScheduledFuture<BackgroundTask>)
                executorService.schedule(runTask(id, task::start), delay, MILLISECONDS);
        taskFutures.put(id, f);
    }

    protected void executeRecurring(String id, BackgroundTask task, long delay, long interval) {
        ScheduledFuture<BackgroundTask> f = (ScheduledFuture<BackgroundTask>) executorService.scheduleAtFixedRate(
                runTask(id, task::start), delay, interval, MILLISECONDS);
        taskFutures.put(id, f);
    }

    /*
    Internal Methods
     */
    private Var serialiseTaskToVar(TaskState state) {
        Var serialised = var("task").isa("scheduled-task")
                                           .has("delay", state.getDelay())
                                           .has("interval", state.getInterval())
                                           .has("task-name", state.getName())
                                           .has("status-change-time", state.getStatusChangeTime().getTime())
                                           .has("queued-time", state.getQueuedTime().getTime());

        // Don't serialise attributes which may be null.
        if(state.getStatusChangedBy() != null)
            serialised.has("status-change-by", state.getStatusChangedBy());

        if(state.getStatusChangeMessage() != null)
            serialised.has("status-change-message", state.getStatusChangeMessage());

        if(state.getCreator() != null)
            serialised.has("created-by", state.getCreator());

        return serialised;
    }

    private Collection<Var> addRelations(Var stateVar, TaskState state) {
        Var taskStatus = var().isa("task-status").has("task-status-value", state.getStatus().toString());
        Var executingHostname = var().isa("executing-hostname").has("executing-hostname-value", hostname);

        Collection<Var> collection = new ArrayList<>();
        collection.add(stateVar);

        collection.add(var().rel("status-of-task-owner", var("task"))
                            .rel("status-of-task-value", taskStatus)
                            .isa("status-of-task"));

        collection.add(var().rel("task-executing-hostname-owner", var("task"))
                            .rel("task-executing-hostname-value", executingHostname)
                            .isa("task-executing-hostname"));

        // Presence of task-recurrence relationship means task is recurring.
        if (state.getRecurring() != null && state.getRecurring())
            collection.add(var().rel("task-recurrence-owner", var("task"))
                    .rel("task-recurrence-value", var().isa("task-is-recurring"))
                    .isa("task-recurrence"));

        return collection;
    }

    private String getVarID(Var var) throws MindmapsValidationException {
        // Query graph for just saved state to get its ID;
        return graph.graql().match(var).execute()
                .get(0).values()
                .stream().findFirst()
                .map(Concept::getId)
                .orElse(null);
    }

    private String getResourceValue(Instance instance, String type) {
        return instance.asEntity()
                       .resources(graph.getResourceType(type))
                       .stream().findFirst()
                       .map(resource -> resource.getValue().toString())
                       .orElse(null);
    }

    private String getRelationValue(String relType, String id, String roleType, String resourceType) {
       return graph.graql().match(var().isa(relType)
                                       .rel(var().id(id))
                                       .rel(roleType, var().has(resourceType, var("x"))))
                .get("x").findFirst()
                .map(concept -> concept.asResource().getValue().toString())
                .orElse(null);
    }

    private Boolean isInRel(String relType, String id, String roleType) {
        return graph.graql().match(var("x").isa(relType)
                                                  .rel(var().id(id))
                                                  .rel(roleType))
                            .get("x").count() >= 1;
    }

    private Runnable runTask(String id, Runnable fn) {
        return () -> {
            TaskState state = getTaskState(id);
            state.setStatus(RUNNING);
            try {
                updateTaskState(id, state);
            } catch (MindmapsValidationException e) {
                LOG.error("Could not update state for task "+state.getName()+
                        " id: "+id+
                        " the error was: "+e.getMessage());
            } finally {
                fn.run();
            }
        };
    }
}
