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

package ai.grakn.engine.backgroundtasks.taskstorage;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Resource;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.TypeName;
import ai.grakn.engine.backgroundtasks.StateStorage;
import ai.grakn.engine.backgroundtasks.TaskState;
import ai.grakn.engine.backgroundtasks.TaskStatus;
import ai.grakn.engine.backgroundtasks.distributed.KafkaLogger;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.factory.GraphFactory;
import ai.grakn.factory.SystemKeyspace;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Var;
import ai.grakn.util.Schema;
import javafx.util.Pair;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static ai.grakn.engine.backgroundtasks.TaskStatus.CREATED;
import static ai.grakn.engine.util.SystemOntologyElements.CREATED_BY;
import static ai.grakn.engine.util.SystemOntologyElements.ENGINE_ID;
import static ai.grakn.engine.util.SystemOntologyElements.RECURRING;
import static ai.grakn.engine.util.SystemOntologyElements.RECUR_INTERVAL;
import static ai.grakn.engine.util.SystemOntologyElements.RUN_AT;
import static ai.grakn.engine.util.SystemOntologyElements.SCHEDULED_TASK;
import static ai.grakn.engine.util.SystemOntologyElements.STACK_TRACE;
import static ai.grakn.engine.util.SystemOntologyElements.STATUS;
import static ai.grakn.engine.util.SystemOntologyElements.STATUS_CHANGE_BY;
import static ai.grakn.engine.util.SystemOntologyElements.STATUS_CHANGE_TIME;
import static ai.grakn.engine.util.SystemOntologyElements.TASK_CHECKPOINT;
import static ai.grakn.engine.util.SystemOntologyElements.TASK_CLASS_NAME;
import static ai.grakn.engine.util.SystemOntologyElements.TASK_CONFIGURATION;
import static ai.grakn.engine.util.SystemOntologyElements.TASK_EXCEPTION;
import static ai.grakn.graql.Graql.var;
import static java.lang.Thread.sleep;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;

public class GraknStateStorage implements StateStorage {
    private final static String TASK_VAR = "task";
    private final static int retries = 10;

    private final KafkaLogger LOG = KafkaLogger.getInstance();

    public GraknStateStorage() {}

    public String newState(String taskName, String createdBy, Date runAt, Boolean recurring, long interval, JSONObject configuration) {
        if(taskName == null || createdBy == null || runAt == null || recurring == null) {
            return null;
        }

        Var state = var(TASK_VAR).isa(SCHEDULED_TASK.getValue())
                                 .has(STATUS.getValue(), CREATED.toString())
                                 .has(TASK_CLASS_NAME.getValue(), taskName)
                                 .has(CREATED_BY.getValue(), createdBy)
                                 .has(RUN_AT.getValue(), runAt.getTime())
                                 .has(RECURRING.getValue(), recurring)
                                 .has(RECUR_INTERVAL.getValue(), interval);

        if(configuration != null) {
            state.has(TASK_CONFIGURATION.getValue(), configuration.toString());
        }

        Optional<String> result = attemptCommitToSystemGraph((graph) -> {
            InsertQuery query = graph.graql().insert(state);
            ConceptId id = query.stream().findFirst().get().get(TASK_VAR).getId();

            LOG.debug("Created " + graph.getConcept(id));

            return id.getValue();
        }, true);

        return result.map(x -> x).orElse(null);
    }

    public Boolean updateState(String id, TaskStatus status, String statusChangeBy, String engineID,
                               Throwable failure, String checkpoint, JSONObject configuration) {
        if(id == null) {
            return false;
        }

        if(status == null && statusChangeBy == null && engineID == null && failure == null
                && checkpoint == null && configuration == null) {
            return false;
        }

        // Existing resource relations to remove
        final Set<TypeName> resourcesToDettach = new HashSet<>();
        
        // New resources to add
        Var resources = var(TASK_VAR).id(ConceptId.of(id));

        if(status != null) {
            resourcesToDettach.add(STATUS);
            resourcesToDettach.add(STATUS_CHANGE_TIME);
            resources.has(STATUS.getValue(), status.toString())
                     .has(STATUS_CHANGE_TIME.getValue(), new Date().getTime());
        }
        if(statusChangeBy != null) {
            resourcesToDettach.add(STATUS_CHANGE_BY);            
            resources.has(STATUS_CHANGE_BY.getValue(), statusChangeBy);
        }
        if(engineID != null) {
            resourcesToDettach.add(ENGINE_ID);
            resources.has(ENGINE_ID.getValue(), engineID);
        }
        if(failure != null) {
            resourcesToDettach.add(TASK_EXCEPTION);
            resourcesToDettach.add(STACK_TRACE);            
            resources.has(TASK_EXCEPTION.getValue(), failure.toString());
            if(failure.getStackTrace().length > 0) {
                resources.has(STACK_TRACE.getValue(), Arrays.toString(failure.getStackTrace()));
            }
        }
        if(checkpoint != null) {
            resourcesToDettach.add(TASK_CHECKPOINT);
            resources.has(TASK_CHECKPOINT.getValue(), checkpoint);
        }
        if(configuration != null) {
            resourcesToDettach.add(TASK_CONFIGURATION);            
            resources.has(TASK_CONFIGURATION.getValue(), configuration.toString());
        }

        Optional<Boolean> result = attemptCommitToSystemGraph((graph) -> {
            LOG.debug("dettaching: " + resourcesToDettach);
            LOG.debug("inserting " + resources);
            final Entity task = graph.getConcept(ConceptId.of(id));
            // Remove relations to any resources we want to currently update
            resourcesToDettach.forEach(typeName -> {
                RoleType roleType = graph.getType(Schema.Resource.HAS_RESOURCE_OWNER.getName(typeName));
                if (roleType == null) {
                    System.err.println("NO ROLE TYPE FOR RESOURCE " + typeName);
                }
                task.relations(roleType).forEach(Concept::delete);
            });
            // Insert new resources with new values
            graph.graql().insert(resources).execute();
            return true;
        }, true);

        return result.isPresent();
    }

    public TaskState getState(String id) {
        if(id == null) {
            return null;
        }

        Optional<TaskState> result = attemptCommitToSystemGraph((graph) -> {
            Instance instance = graph.getConcept(ConceptId.of(id));
            return instanceToState(graph, instance);
        }, false);

        return result.get();
    }

    private TaskState instanceToState(GraknGraph graph, Instance instance){
        Resource<?> name = instance.resources(graph.getType(TASK_CLASS_NAME)).stream().findFirst().orElse(null);
        if (name == null) {
            LOG.error("Could not get 'task-class-name' for " + instance.getId());
            return null;
        }

        TaskState state = new TaskState(name.getValue().toString());

        List<Map<String, Concept>> resources = graph.graql()
                .match(var().rel(var().id(instance.getId())).rel(var("r").isa(var().sub("resource"))))
                .select("r")
                .execute();

        resources.forEach(x -> x.values().forEach(y -> {
            Resource<?> r = y.asResource();
            buildState(state, r.type().getName(), r.getValue());
        }));

        return state;
    }

    public Set<Pair<String, TaskState>> getTasks(TaskStatus taskStatus, String taskClassName, String createdBy,
                                                 int limit, int offset) {
        return getTasks(taskStatus, taskClassName, createdBy, limit, offset, false);
    }

    public Set<Pair<String, TaskState>> getTasks(TaskStatus taskStatus, String taskClassName, String createdBy,
                                                 int limit, int offset, Boolean recurring) {
        Var matchVar = var(TASK_VAR).isa(SCHEDULED_TASK.getValue());

        if(taskStatus != null) {
            matchVar.has(STATUS.getValue(), taskStatus.toString());
        }
        if(taskClassName != null) {
            matchVar.has(TASK_CLASS_NAME.getValue(), taskClassName);
        }
        if(createdBy != null) {
            matchVar.has(CREATED_BY.getValue(), createdBy);
        }
        if(recurring != null) {
            matchVar.has(RECURRING.getValue(), recurring);
        }

        Optional<Set<Pair<String, TaskState>>> result = attemptCommitToSystemGraph((graph) -> {
            MatchQuery q = graph.graql().match(matchVar);

            if (limit > 0) {
                q.limit(limit);
            }
            if (offset > 0) {
                q.offset(offset);
            }

            List<Map<String, Concept>> res = q.execute();

            // Create Set of pairs with IDs &
            Set<Pair<String, TaskState>> out = new HashSet<>();
            for (Map<String, Concept> m : res) {
                Concept c = m.values().stream().findFirst().orElse(null);
                if (c != null) {
                    String id = c.getId().getValue();
                    out.add(new Pair<>(id, instanceToState(graph, c.asInstance())));
                }
            }

            return out;
        }, false);

        return result.isPresent() ? result.get() : new HashSet<>();
    }

    /*
    Internal
     */
    private TaskState buildState(TaskState state, TypeName resourceName, Object resourceValue) {
            if (resourceName.equals(STATUS)) {
                return state.status(TaskStatus.valueOf(resourceValue.toString()));
            }
            if (resourceName.equals(STATUS_CHANGE_TIME)) {
                return state.statusChangeTime(new Date((Long) resourceValue));
            }
            if (resourceName.equals(STATUS_CHANGE_BY)) {
                return state.statusChangedBy(resourceValue.toString());
            }
            if (resourceName.equals(TASK_CLASS_NAME)) {
                // Set when instantiating TaskState, ignore it now.
                return state;
            }
            if (resourceName.equals(CREATED_BY)) {
                return state.creator(resourceValue.toString());
            }
            if (resourceName.equals(ENGINE_ID)) {
                return state.engineID(resourceValue.toString());
            }
            if (resourceName.equals(RUN_AT)) {
                return state.runAt(new Date((Long) resourceValue));
            }
            if (resourceName.equals(RECURRING)) {
                return state.isRecurring((Boolean) resourceValue);
            }
            if (resourceName.equals(RECUR_INTERVAL)) {
                return state.interval((Long) resourceValue);
            }
            if (resourceName.equals(TASK_EXCEPTION)) {
                return state.exception(resourceValue.toString());
            }
            if (resourceName.equals(STACK_TRACE)) {
                return state.stackTrace(resourceValue.toString());
            }
            if (resourceName.equals(TASK_CHECKPOINT)) {
                return state.checkpoint(resourceValue.toString());
            }
            if (resourceName.equals(TASK_CONFIGURATION)) {
                return state.configuration(new JSONObject(resourceValue.toString()));
            }

            LOG.error("Unknown resource type when deserialising TaskState: " + resourceName);
            return state;
        }

    private synchronized <T> Optional<T> attemptCommitToSystemGraph(Function<GraknGraph, T> function, boolean commit){
        double sleepFor = 100;
        for (int i = 0; i < retries; i++) {

            LOG.debug("Attempting "  + (commit ? "commit" : "query") + " on system graph @ t"+Thread.currentThread().getId());
            long time = System.currentTimeMillis();

            try (GraknGraph graph = GraphFactory.getInstance().getGraph(SystemKeyspace.SYSTEM_GRAPH_NAME)) {
                T result = function.apply(graph);
                if (commit) {
                    graph.commit();
                }

                return Optional.of(result);
            } 
            catch (GraknBackendException e) {
                // retry...
            }            
            catch (Throwable e) {
                e.printStackTrace(System.err);
                LOG.error("Failed to validate the graph when updating the state " + getFullStackTrace(e));
                break;
            } 
            finally {
                LOG.debug("Took " + (System.currentTimeMillis() - time) + " to " + (commit ? "commit" : "query") + " to system graph @ t" + Thread.currentThread().getId());
            }

            // Sleep
            try {
                sleep((long)sleepFor);
            }
            catch (InterruptedException e) {
                LOG.error(getFullStackTrace(e));
            }
            finally {
                sleepFor = ((1d/2d) * (Math.pow(2d,i) - 1d));
            }
        }

        return Optional.empty();
    }
}
