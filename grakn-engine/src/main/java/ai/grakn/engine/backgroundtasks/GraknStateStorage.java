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

package ai.grakn.engine.backgroundtasks;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.factory.GraphFactory;
import ai.grakn.graql.Var;
import javafx.util.Pair;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.function.Function;

import static ai.grakn.graql.Graql.var;

public class GraknStateStorage implements StateStorage {
    private final static String TASK_VAR = "task";

    private final Logger LOG = LoggerFactory.getLogger(GraknStateStorage.class);
    private GraknGraph graph;

    public GraknStateStorage() {
        graph = GraphFactory.getInstance().getGraph(ConfigProperties.SYSTEM_GRAPH_NAME);
    }

    public String newState(String taskName, String createdBy, Date runAt, Boolean recurring, long interval, JSONObject configuration) {
        if(taskName == null || createdBy == null || runAt == null || recurring == null)
            return null;

        Var state = var("task").isa("scheduled-task")
                                      .has("task-class-name", taskName)
                                      .has("created-by", createdBy)
                                      .has("run-at", runAt.getTime())
                                      .has("recurring", recurring)
                                      .has("recur-interval", interval);

        if(configuration != null)
            state.has("task-configuration", configuration.toString());

        graph.graql().insert(state).execute();

        try {
            graph.commit();
        } catch (GraknValidationException e) {
            LOG.error("Could not commit task to graph: "+e.getMessage());
            return null;
        }

        return graph.graql().match(state).execute()
                    .get(0).values()
                    .stream().findFirst()
                    .map(Concept::getId)
                    .orElse(null);
    }

    public void updateState(String id, TaskStatus status, String statusChangeBy, String executingHostname,
                            Throwable failure, String checkpoint, JSONObject configuration) {
        if(id == null)
            return;

        if(status == null && statusChangeBy == null && executingHostname == null && failure == null
                && checkpoint == null && configuration == null)
            return;

        // Existing resource relations to remove
        Var deleters = var(TASK_VAR);

        // New resources to add
        Var resources = var(TASK_VAR).id(id);

        if(status != null) {
            deleters.has("status");
            resources.has("status", status.toString());
        }
        if(statusChangeBy != null) {
            deleters.has("status-change-by");
            resources.has("status-change-by", statusChangeBy);
        }
        if(executingHostname != null) {
            deleters.has("executing-hostname");
            resources.has("executing-hostname", executingHostname);
        }
        if(failure != null) {
            deleters.has("task-failure");
            resources.has("task-failure", serialise(failure));
        }
        if(checkpoint != null) {
            deleters.has("task-checkpoint");
            resources.has("task-checkpoint", checkpoint);
        }
        if(configuration != null) {
            deleters.has("task-configuration");
            resources.has("task-configuration", configuration.toString());
        }

        // Remove relations to any resources we want to currently update
        graph.graql().match(var(TASK_VAR).id(id))
                .delete(deleters)
                .execute();

        // Insert new resources with new values.
        graph.graql().insert(resources)
                .execute();

        try {
            graph.commit();
        } catch(GraknValidationException e) {
            e.printStackTrace();
        }
    }

    public TaskState getState(String id) {
        if(id == null)
            return null;

        Instance instance = graph.getInstance(id);
        Resource<?> name = instance.resources(graph.getResourceType("task-class-name")).stream().findFirst().orElse(null);
        if(name == null) {
            LOG.error("Could not get 'task-class-name' for "+id);
            return null;
        }

        TaskState state = new TaskState(name.getValue().toString());

        List<Map<String, Concept>> resources = graph.graql().match(var().rel(var().id(id)).rel(var("r").isa(var().isa("resource-type"))))
                .select("r")
                .execute();

        resources.forEach(x -> x.values().forEach(y -> {
            Resource<?> r = y.asResource();
            buildState(state, r.type().getId(), r.getValue());
        }));

        return state;
    }

    public Set<Pair<String, TaskState>> getTasks(TaskStatus taskStatus, String taskClassName, String createdBy, int limit, int offset) {
        return null;
    }


    /*
    Internal
     */
    private TaskState buildState(TaskState state, String resourceName, Object resourceValue) {
        switch (resourceName) {
            case "status":
                state.status(TaskStatus.valueOf(resourceValue.toString()));
                break;
            case "status-change-time":
                state.statusChangeTime(new Date((Long)resourceValue));
                break;
            case "status-change-by":
                state.statusChangedBy(resourceValue.toString());
                break;
            case "task-class-name":
                // Set when instantiating TaskState, ignore it now.
                break;
            case "created-by":
                state.creator(resourceValue.toString());
                break;
            case "executing-hostname":
                state.executingHostname(resourceValue.toString());
                break;
            case "run-at":
                state.runAt(new Date((Long)resourceValue));
                break;
            case "recurring":
                state.isRecurring((Boolean)resourceValue);
                break;
            case "recur-interval":
                state.interval((Long)resourceValue);
                break;
            case "task-failure":
                state.failure(deserialise(resourceValue.toString()));
                break;
            case "task-checkpoint":
                state.checkpoint(resourceValue.toString());
                break;
            case "task-configuration":
                state.configuration(new JSONObject(resourceValue.toString()));
                break;
            default:
                LOG.error("Unknown resource type when deserialising TaskState: "+resourceName);
                break;
        }

        return state;
    }

    private String serialise(Object o) {
        String serialised = null;

        try {
            ByteArrayOutputStream bytestream = new ByteArrayOutputStream();
            ObjectOutputStream outputStream = new ObjectOutputStream(bytestream);

            outputStream.writeObject(o);
            outputStream.flush();
            serialised = Base64.getEncoder().encodeToString(bytestream.toByteArray());
        }
        catch (IOException e) {
            LOG.error("Could not serialise object: "+o+"\n the error was: "+e);
        }

        return serialised;
    }

    private <T> T deserialise(String s) {
        T deserialised = null;

        try {
            byte data[] = Base64.getDecoder().decode(s);
            ByteArrayInputStream byteStream = new ByteArrayInputStream(data);
            ObjectInputStream inputStream = new ObjectInputStream(byteStream);
            deserialised = (T) inputStream.readObject();
        }
        catch(IOException | ClassNotFoundException e ) {
            LOG.error("Could not deserialise object: "+s+"\n the error was: "+e);
        }

        return deserialised;
    }

}
