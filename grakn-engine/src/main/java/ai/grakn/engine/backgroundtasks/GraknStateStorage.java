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

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Resource;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.factory.GraphFactory;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Var;
import javafx.util.Pair;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static ai.grakn.engine.backgroundtasks.TaskStatus.CREATED;
import static ai.grakn.engine.util.SystemOntologyElements.*;
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

        Var state = var(TASK_VAR).isa(SCHEDULED_TASK)
                                 .has(STATUS, CREATED.toString())
                                 .has(TASK_CLASS_NAME, taskName)
                                 .has(CREATED_BY, createdBy)
                                 .has(RUN_AT, runAt.getTime())
                                 .has(RECURRING, recurring)
                                 .has(RECUR_INTERVAL, interval);

        if(configuration != null)
            state.has(TASK_CONFIGURATION, configuration.toString());

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
            deleters.has(STATUS)
                    .has(STATUS_CHANGE_TIME);
            resources.has(STATUS, status.toString())
                     .has(STATUS_CHANGE_TIME, new Date().getTime());
        }
        if(statusChangeBy != null) {
            deleters.has(STATUS_CHANGE_BY);
            resources.has(STATUS_CHANGE_BY, statusChangeBy);
        }
        if(executingHostname != null) {
            deleters.has(EXECUTING_HOSTNAME);
            resources.has(EXECUTING_HOSTNAME, executingHostname);
        }
        if(failure != null) {
            deleters.has(TASK_EXCEPTION)
                    .has(STACK_TRACE);

            resources.has(TASK_EXCEPTION, failure.toString());
            if(failure.getStackTrace().length > 0)
                 resources.has(STACK_TRACE, Arrays.toString(failure.getStackTrace()));
        }
        if(checkpoint != null) {
            deleters.has(TASK_CHECKPOINT);
            resources.has(TASK_CHECKPOINT, checkpoint);
        }
        if(configuration != null) {
            deleters.has(TASK_CONFIGURATION);
            resources.has(TASK_CONFIGURATION, configuration.toString());
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

        Instance instance = graph.getConcept(id);
        Resource<?> name = instance.resources(graph.getResourceType(TASK_CLASS_NAME)).stream().findFirst().orElse(null);
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
            buildState(state, r.type().getName(), r.getValue());
        }));

        return state;
    }

    public Set<Pair<String, TaskState>> getTasks(TaskStatus taskStatus, String taskClassName, String createdBy, int limit, int offset) {
        Var matchVar = var(TASK_VAR).isa(SCHEDULED_TASK);

        if(taskStatus != null)
            matchVar.has(STATUS, taskStatus.toString());
        if(taskClassName != null)
            matchVar.has(TASK_CLASS_NAME, taskClassName);
        if(createdBy != null)
            matchVar.has(CREATED_BY, createdBy);

        MatchQuery q = graph.graql().match(matchVar);

        if(limit > 0)
            q.limit(limit);
        if(offset > 0)
            q.offset(offset);

        List<Map<String, Concept>> res = q.execute();

        // Create Set of pairs with IDs &
        Set<Pair<String, TaskState>> out = new HashSet<>();
        for(Map<String, Concept> m: res) {
            Concept c = m.values().stream().findFirst().orElse(null);
            if(c != null) {
                String id = c.getId();

                out.add(new Pair<>(id, getState(id)));
            }
        }

        return out;
    }

    /*
    Internal
     */
    private TaskState buildState(TaskState state, String resourceName, Object resourceValue) {
        switch (resourceName) {
            case STATUS:
                state.status(TaskStatus.valueOf(resourceValue.toString()));
                break;
            case STATUS_CHANGE_TIME:
                state.statusChangeTime(new Date((Long)resourceValue));
                break;
            case STATUS_CHANGE_BY:
                state.statusChangedBy(resourceValue.toString());
                break;
            case TASK_CLASS_NAME:
                // Set when instantiating TaskState, ignore it now.
                break;
            case CREATED_BY:
                state.creator(resourceValue.toString());
                break;
            case EXECUTING_HOSTNAME:
                state.executingHostname(resourceValue.toString());
                break;
            case RUN_AT:
                state.runAt(new Date((Long)resourceValue));
                break;
            case RECURRING:
                state.isRecurring((Boolean)resourceValue);
                break;
            case RECUR_INTERVAL:
                state.interval((Long)resourceValue);
                break;
            case TASK_EXCEPTION:
                state.exception(resourceValue.toString());
                break;
            case STACK_TRACE:
                state.stackTrace(resourceValue.toString());
                break;
            case TASK_CHECKPOINT:
                state.checkpoint(resourceValue.toString());
                break;
            case TASK_CONFIGURATION:
                state.configuration(new JSONObject(resourceValue.toString()));
                break;
            default:
                LOG.error("Unknown resource type when deserialising TaskState: "+resourceName);
                break;
        }

        return state;
    }
}
