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

import ai.grakn.engine.TaskStatus;
import ai.grakn.engine.TaskId;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.engine.tasks.manager.ZookeeperConnection;
import ai.grakn.engine.util.EngineID;
import ai.grakn.exception.EngineStorageException;
import org.apache.curator.framework.api.transaction.CuratorTransactionBridge;

import java.util.Set;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.lang.SerializationUtils.deserialize;
import static org.apache.commons.lang.SerializationUtils.serialize;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;

/**
 * <p>
 * Manages the state of background {@link ai.grakn.engine.tasks.BackgroundTask} in
 * a synchronized manner withing a cluster.
 * </p>
 *
 * @author alexandraorth
 */
public class TaskStateZookeeperStore implements TaskStateStorage {

    private static final String ALL_TASKS = "/tasks";
    private static final String SINGLE_TASK = "/tasks/%s";
    private final ZookeeperConnection zookeeper;

    public TaskStateZookeeperStore(ZookeeperConnection zookeeper) {
        this.zookeeper = zookeeper;
    }

    /**
     * Creates a new task state in Zookeeper
     *
     * @param task The new task to create
     * @return Identifier of the created task
     */
    @Override
    public TaskId newState(TaskState task){
        try {
            // Write the current serialized task
            zookeeper.connection().create().creatingParentContainersIfNeeded().forPath(taskPath(task), serialize(task));

            return task.getId();
        } catch (Exception exception){
            throw new EngineStorageException(exception);
        }
    }

    /**
     * Writes a new state to Zookeeper.
     *
     * Paths updated:
     *  + /tasks/id/state
     *
     * @param task State to update in Zookeeper
     * @return True if successfully update, false otherwise
     */
    @Override
    public Boolean updateState(TaskState task){
        try {
            // Start a transaction to write the current serialized task
            CuratorTransactionBridge transaction = zookeeper.connection().inTransaction()
                    .setData().forPath(taskPath(task), serialize(task));

            // Execute transaction
            transaction.and().commit();

            return true;
        } catch (Exception e){
            throw new EngineStorageException(e);
        }
    }

    /**
     * Retrieve the TaskState associated with the given ID.
     *
     * @param id String id of task.
     * @return State of the given task
     */
    @Override
    public TaskState getState(TaskId id) {
        try {
            byte[] stateInZk = zookeeper.connection().getData().forPath(taskPath(id));
            return (TaskState) deserialize(stateInZk);
        } catch (Exception exception){
            throw new EngineStorageException(exception);
        }
    }

    @Override
    public boolean containsTask(TaskId id) {
        try {
            return zookeeper.connection().checkExists().forPath(taskPath(id)) != null;
        } catch (Exception exception){
            throw new EngineStorageException(exception);
        }
    }

    /**
     * This implementation will fetch all of the tasks from zookeeper and then
     * filer them out.
     *
     * ZK stores tasks by ID, so at the moment there is no more efficient way to search
     * within the storage itself.
     */
    @Override
    public Set<TaskState> getTasks(TaskStatus taskStatus, String taskClassName, String createdBy, EngineID engineRunningOn, int limit, int offset){
        try {
            Stream<TaskState> stream = zookeeper.connection().getChildren()
                        .forPath(ALL_TASKS).stream()
                        .map(TaskId::of)
                        .map(this::getState);

            if (taskStatus != null) {
                stream = stream.filter(t -> t.status().equals(taskStatus));
            }

            if (taskClassName != null) {
                stream = stream.filter(t -> t.taskClass().getName().equals(taskClassName));
            }

            if (createdBy != null) {
                stream = stream.filter(t -> t.creator().equals(createdBy));
            }

            if (engineRunningOn != null) {
                stream = stream.filter(t -> t.engineID() != null && t.engineID().equals(engineRunningOn));
            }

            stream = stream.skip(offset);

            if(limit > 0){
                stream = stream.limit(limit);
            }

            return stream.collect(toSet());
        } catch (Exception e){
            throw new EngineStorageException("Could not get state from storage " + getFullStackTrace(e));
        }
    }

    /**
     * Path the return a single task
     * @return Zookeeper path for a single task
     */
    private String taskPath(TaskState task){
        return taskPath(task.getId());
    }

    /**
     * Path the return a single task
     * @return Zookeeper path for a single task
     */
    private String taskPath(TaskId id){
        return format(SINGLE_TASK, id);
    }
}
