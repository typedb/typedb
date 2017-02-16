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

package ai.grakn.engine.tasks.storage;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Instance;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.TypeName;
import ai.grakn.engine.TaskStatus;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.engine.postprocessing.EngineCache;
import ai.grakn.exception.EngineStorageException;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.factory.EngineGraknGraphFactory;
import ai.grakn.factory.SystemKeyspace;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Var;
import ai.grakn.util.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static ai.grakn.engine.TaskStatus.CREATED;
import static ai.grakn.engine.util.SystemOntologyElements.CREATED_BY;
import static ai.grakn.engine.util.SystemOntologyElements.ENGINE_ID;
import static ai.grakn.engine.util.SystemOntologyElements.RECURRING;
import static ai.grakn.engine.util.SystemOntologyElements.RECUR_INTERVAL;
import static ai.grakn.engine.util.SystemOntologyElements.RUN_AT;
import static ai.grakn.engine.util.SystemOntologyElements.SCHEDULED_TASK;
import static ai.grakn.engine.util.SystemOntologyElements.SERIALISED_TASK;
import static ai.grakn.engine.util.SystemOntologyElements.STACK_TRACE;
import static ai.grakn.engine.util.SystemOntologyElements.STATUS;
import static ai.grakn.engine.util.SystemOntologyElements.STATUS_CHANGE_BY;
import static ai.grakn.engine.util.SystemOntologyElements.STATUS_CHANGE_TIME;
import static ai.grakn.engine.util.SystemOntologyElements.TASK_CHECKPOINT;
import static ai.grakn.engine.util.SystemOntologyElements.TASK_CLASS_NAME;
import static ai.grakn.engine.util.SystemOntologyElements.TASK_CONFIGURATION;
import static ai.grakn.engine.util.SystemOntologyElements.TASK_EXCEPTION;
import static ai.grakn.engine.util.SystemOntologyElements.TASK_ID;
import static ai.grakn.graql.Graql.name;
import static ai.grakn.graql.Graql.var;
import static java.lang.Thread.sleep;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;

/**
 * <p>
 *     Implementation of StateStorage that stores task state in the Grakn system graph.
 * </p>
 *
 * @author Denis Lobanov, alexandraorth
 */
public class TaskStateGraphStore implements TaskStateStorage {
    private final static String TASK_VAR = "task";
    private final static int retries = 10;

    private final Logger LOG = LoggerFactory.getLogger(TaskStateGraphStore.class);

    public TaskStateGraphStore() {}

    @Override
    public String newState(TaskState task) throws EngineStorageException {
        Var state = var(TASK_VAR).isa(name(SCHEDULED_TASK))
                .has(TASK_ID.getValue(), task.getId())
                .has(STATUS, var().value(CREATED.toString()))
                .has(TASK_CLASS_NAME, var().value(task.taskClassName()))
                .has(CREATED_BY, var().value(task.creator()))
                .has(RUN_AT, var().value(task.runAt().toEpochMilli()))
                .has(RECURRING, var().value(task.isRecurring()))
                .has(RECUR_INTERVAL, var().value(task.interval()))
                .has(SERIALISED_TASK, var().value(TaskState.serialize(task)));

        if(task.configuration() != null) {
            state.has(TASK_CONFIGURATION, var().value(task.configuration().toString()));
        }

        Optional<Boolean> result = attemptCommitToSystemGraph((graph) -> {
            graph.graql().insert(state).execute();
            return true;
        }, true);

        if(!result.isPresent()){
            throw new EngineStorageException("Concept " + task.getId() + " could not be saved in storage");
        }

        return task.getId();
    }

    @Override
    public Boolean updateState(TaskState task) {
        // Existing resource relations to remove
        final Set<TypeName> resourcesToDettach = new HashSet<>();
        
        // New resources to add
        Var resources = var(TASK_VAR);

        resourcesToDettach.add(SERIALISED_TASK);
        resources.has(SERIALISED_TASK, var().value(TaskState.serialize(task)));

        // TODO make sure all properties are being update
        if(task.status() != null) {
            resourcesToDettach.add(STATUS);
            resourcesToDettach.add(STATUS_CHANGE_TIME);
            resources.has(STATUS, var().value(task.status().toString()))
                     .has(STATUS_CHANGE_TIME, var().value(new Date().getTime()));
        }
        if(task.statusChangedBy() != null) {
            resourcesToDettach.add(STATUS_CHANGE_BY);            
            resources.has(STATUS_CHANGE_BY, var().value(task.statusChangedBy()));
        }
        if(task.engineID() != null) {
            resourcesToDettach.add(ENGINE_ID);
            resources.has(ENGINE_ID, var().value(task.engineID()));
        }
        if(task.exception() != null) {
            resourcesToDettach.add(TASK_EXCEPTION);
            resourcesToDettach.add(STACK_TRACE);            
            resources.has(TASK_EXCEPTION, var().value(task.exception()));
            if(task.stackTrace() != null) {
                resources.has(STACK_TRACE, var().value(task.stackTrace()));
            }
        }
        if(task.checkpoint() != null) {
            resourcesToDettach.add(TASK_CHECKPOINT);
            resources.has(TASK_CHECKPOINT, var().value(task.checkpoint()));
        }
        if(task.configuration() != null) {
            resourcesToDettach.add(TASK_CONFIGURATION);            
            resources.has(TASK_CONFIGURATION, var().value(task.configuration().toString()));
        }

        Optional<Boolean> result = attemptCommitToSystemGraph((graph) -> {
            Instance taskConcept = graph.getResourcesByValue(task.getId()).iterator().next().owner();
            // Remove relations to any resources we want to currently update
            resourcesToDettach.forEach(typeName -> {
                RoleType roleType = graph.getType(Schema.Resource.HAS_RESOURCE_OWNER.getName(typeName));
                taskConcept.relations(roleType).forEach(Concept::delete);
            });

            resources.id(taskConcept.getId());

            // Insert new resources with new values
            graph.graql().insert(resources).execute();
            return true;
        }, true);

        return result.isPresent();
    }

    @Override
    public TaskState getState(String id) throws EngineStorageException {
        Optional<TaskState> result = attemptCommitToSystemGraph((graph) -> {
            Instance instance = graph.getResourcesByValue(id).iterator().next().owner();
            return instanceToState(graph, instance);
        }, false);

        if(!result.isPresent()){
            throw new EngineStorageException("Concept " + id + " not found in storage");
        }

        return result.get();
    }

    /**
     * Given an instance concept, turn it into a TaskState object.
     * This is done by retrieving the serialized TaskState from the given graph and deserialising it.
     *
     * @param graph Graph in which to fetch serialized state
     * @param instance Task instance to turn into task state
     * @return TaskState representing given instance
     */
    public TaskState instanceToState(GraknGraph graph, Instance instance){
        ResourceType<String> serialisedResourceType = graph.getResourceType(SERIALISED_TASK.getValue());
        String serialisedTask = (String) instance.resources(serialisedResourceType).iterator().next().getValue();

        return TaskState.deserialize(serialisedTask);
    }

    @Override
    public Set<TaskState> getTasks(TaskStatus taskStatus, String taskClassName, String createdBy,
                                                 int limit, int offset) {
        return getTasks(taskStatus, taskClassName, createdBy, limit, offset, false);
    }

    public Set<TaskState> getTasks(TaskStatus taskStatus, String taskClassName, String createdBy,
                                                 int limit, int offset, Boolean recurring) {
        Var matchVar = var(TASK_VAR).isa(name(SCHEDULED_TASK));

        if(taskStatus != null) {
            matchVar.has(STATUS, var().value(taskStatus.toString()));
        }
        if(taskClassName != null) {
            matchVar.has(TASK_CLASS_NAME, var().value(taskClassName));
        }
        if(createdBy != null) {
            matchVar.has(CREATED_BY, var().value(createdBy));
        }
        if(recurring != null) {
            matchVar.has(RECURRING, var().value(recurring));
        }

        Optional<Set<TaskState>> result = attemptCommitToSystemGraph((graph) -> {
            MatchQuery q = graph.graql().match(matchVar);

            if (limit > 0) {
                q.limit(limit);
            }
            if (offset > 0) {
                q.offset(offset);
            }

            return q.execute().stream()
                    .map(map -> map.values().stream().findFirst())
                    .map(Optional::get)
                    .map(c -> instanceToState(graph, c.asInstance()))
                    .collect(toSet());
        }, false);

        return result.isPresent() ? result.get() : new HashSet<>();
    }

    private <T> Optional<T> attemptCommitToSystemGraph(Function<GraknGraph, T> function, boolean commit){
        double sleepFor = 100;
        for (int i = 0; i < retries; i++) {

            LOG.debug("Attempting "  + (commit ? "commit" : "query") + " on system graph @ t"+Thread.currentThread().getId());
            long time = System.currentTimeMillis();

            try (GraknGraph graph = EngineGraknGraphFactory.getInstance().getGraph(SystemKeyspace.SYSTEM_GRAPH_NAME)) {
                T result = function.apply(graph);
                if (commit) {
                    EngineCache engineCache = EngineCache.getInstance();
                    graph.admin().commit(engineCache.getResourceJobs(graph.getKeyspace()), engineCache.getCastingJobs(graph.getKeyspace()));
                }

                return Optional.of(result);
            } 
            catch (GraknBackendException e) {
                // retry...
                LOG.debug("Trouble inserting " + getFullStackTrace(e));
            }            
            catch (Throwable e) {
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
