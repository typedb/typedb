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
import ai.grakn.engine.tasks.manager.singlequeue.SingleQueueTaskRunner;
import ai.grakn.engine.util.EngineID;
import ai.grakn.exception.EngineStorageException;
import org.apache.curator.framework.api.transaction.CuratorTransactionBridge;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.engine.tasks.config.ZookeeperPaths.SINGLE_ENGINE_PATH;
import static ai.grakn.engine.tasks.config.ZookeeperPaths.TASKS_PATH_PREFIX;
import static ai.grakn.engine.tasks.config.ZookeeperPaths.ZK_ENGINE_TASK_PATH;
import static ai.grakn.engine.tasks.config.ZookeeperPaths.ZK_TASK_PATH;
import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.lang.SerializationUtils.deserialize;
import static org.apache.commons.lang.SerializationUtils.serialize;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;

/**
 * <p>
 * Manages the state of background {@link ai.grakn.engine.tasks.BackgroundTask} in
 * a synchronized manner withing a cluster. This means that all updates must be performed
 * by acquiring a distributed mutex so that no concurrent writes are possible. 
 * </p>
 *
 * //TODO Re-do this class to make it readable
 * @author alexandraorth
 */
public class TaskStateZookeeperStore implements TaskStateStorage {
    private final static Logger LOG = LoggerFactory.getLogger(SingleQueueTaskRunner.class);
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
            // Start a transaction to write the current serialized task
            CuratorTransactionBridge transaction = zookeeper.connection().inTransaction()
                    .create().forPath(taskPath(task), serialize(task));

            // Register this task with the appropriate engine
            registerTaskIsExecutingOnEngine(transaction, null, task.engineID(), task);

            // Execute
            transaction.and().commit();

            return task.getId();
        } catch (Exception exception){
            throw new EngineStorageException(exception);
        }
    }

    /**
     * Writes a new state to Zookeeper and records the engine that task is running on. If no engine
     * is assigned to that task it will delete any existing entries.
     *
     * Paths updated:
     *  + /tasks/id/state
     *  + /engine/tasks/id
     *
     * @param task State to update in Zookeeper
     * @return True if successfully update, false otherwise
     */
    @Override
    public Boolean updateState(TaskState task){
        try {
            // Get the previously stored task
            TaskState previousTask = (TaskState) deserialize(zookeeper.connection().getData().forPath(taskPath(task)));

                // Start a transaction to write the current serialized task
                CuratorTransactionBridge transaction = zookeeper.connection().inTransaction()
                        .setData().forPath(taskPath(task), serialize(task));

                EngineID currentEngineId = task.engineID();
                EngineID previousEngineId = previousTask.engineID();

                registerTaskIsExecutingOnEngine(transaction, previousEngineId, currentEngineId, task);

                // Execute transaction
                transaction.and().commit();

                return true;
        } catch (Exception e){
            throw new EngineStorageException(e);
        }
    }

    /**
     * Retrieve the TaskState associated with the given ID. Acquire a distributed mutex before executing
     * to ensure most up-to-date state.
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
            Stream<TaskState> stream;
            if(engineRunningOn != null){
                stream = zookeeper.connection().getChildren()
                        .forPath(enginePath(engineRunningOn)).stream()
                        .map(TaskId::of)
                        .map(this::getState);
            } else {
                stream = zookeeper.connection().getChildren()
                        .forPath(TASKS_PATH_PREFIX).stream()
                        .map(TaskId::of)
                        .map(this::getState);
            }

            if (taskStatus != null) {
                stream = stream.filter(t -> t.status().equals(taskStatus));
            }

            if (taskClassName != null) {
                stream = stream.filter(t -> t.taskClass().getName().equals(taskClassName));
            }

            if (createdBy != null) {
                stream = stream.filter(t -> t.creator().equals(createdBy));
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

    private void registerTaskIsExecutingOnEngine(
            CuratorTransactionBridge transaction, EngineID previous, EngineID current, TaskState task) throws Exception {

        // If previous engine is non null and this one is non null, delete previous
        if (previous != null) {
            transaction.and().delete().forPath(engineTaskPath(previous, task));
        }

        // If there is a new engine, add it
        if (current != null) {

            // Ensure there is a path for the current engine
            try {
                zookeeper.connection().create().creatingParentContainersIfNeeded().forPath(enginePath(current));
            } catch (KeeperException.NodeExistsException e){
                LOG.trace("Engine {} registered in ZK", current);
            }

            transaction.and().create().forPath(engineTaskPath(current, task));
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
        return format(ZK_TASK_PATH, id);
    }

    /**
     * Path representing the task running on an engine
     * @param engineId Identifier of the engine
     * @param taskState Identifier of the task
     * @return Path representing conbination between engine and task
     */
    private String engineTaskPath(EngineID engineId, TaskState taskState){
        return format(ZK_ENGINE_TASK_PATH, engineId.value(), taskState.getId());
    }

    /**
     * Path to a single engine
     * @param engineId Identifier of the engine
     * @return Path to the engine
     */
    private String enginePath(EngineID engineId){
        return format(SINGLE_ENGINE_PATH, engineId.value());
    }
}
