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

import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.TaskStatus;
import ai.grakn.engine.tasks.manager.ZookeeperConnection;
import ai.grakn.exception.EngineStorageException;
import org.apache.curator.framework.api.transaction.CuratorTransactionBridge;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.engine.tasks.config.ZookeeperPaths.TASKS_PATH_PREFIX;
import static ai.grakn.engine.tasks.config.ZookeeperPaths.ENGINE_PATH;
import static ai.grakn.engine.tasks.config.ZookeeperPaths.TASK_STATE_SUFFIX;
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
 * @author Denis Lobanov, Alexandra Orth
 *
 */
public class TaskStateZookeeperStore implements TaskStateStorage {
    private static final String ZK_TASK_PATH =  TASKS_PATH_PREFIX + "/%s";
    private static final String ZK_ENGINE_PATH = TASKS_PATH_PREFIX + ENGINE_PATH + "/%s";
    private static final String ZK_ENGINE_TASK_PATH =  TASKS_PATH_PREFIX + ENGINE_PATH + "/%s/%s";

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
    public String newState(TaskState task){
       return executeWithMutex(task.getId(), () -> {
            zookeeper.connection().inTransaction()
                    .create().forPath(taskPath(task), serialize(task))
                    .and().commit();
           return task.getId();
        });
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
        return executeWithMutex(task.getId(), () -> {

            String currentEngineId = task.engineID();
            String previousEngineId = ((TaskState) deserialize(zookeeper.read(taskPath(task.getId())))).engineID();

            CuratorTransactionBridge baseTransaction = zookeeper.connection().inTransaction()
                    .setData().forPath(taskPath(task), serialize(task));

            // If the Engine was deleted
            if(currentEngineId == null && previousEngineId != null){
                baseTransaction = baseTransaction.and()
                        .delete().forPath(engineTaskPath(previousEngineId, task));
            }
            // If the engine has been updated
            else if(previousEngineId != null && !Objects.equals(currentEngineId, previousEngineId)){

                if(zookeeper.connection().checkExists().forPath(enginePath(currentEngineId)) != null){
                    zookeeper.connection().create().creatingParentContainersIfNeeded().forPath(enginePath(currentEngineId));
                }

                baseTransaction = baseTransaction.and()
                        .delete().forPath(engineTaskPath(previousEngineId, task)).and()
                        .create().forPath(engineTaskPath(currentEngineId, task));
            }
            // If the engine was added
            else if(currentEngineId != null) {
                if(zookeeper.connection().checkExists().forPath(enginePath(currentEngineId)) == null){
                    zookeeper.connection().create().creatingParentContainersIfNeeded().forPath(enginePath(currentEngineId));
                }

                baseTransaction = baseTransaction.and()
                        .create().forPath(engineTaskPath(currentEngineId, task));
            }

            baseTransaction.and().commit();

            return true;
        });
    }

    /**
     * Retrieve the TaskState associated with the given ID. Acquire a distributed mutex before executing
     * to ensure most up-to-date state.
     *
     * @param id String id of task.
     * @return State of the given task
     */
    @Override
    public TaskState getState(String id) {
        return executeWithMutex(id, () -> (TaskState) deserialize(zookeeper.read(taskPath(id))));
    }

    /**
     * This implementation will fetch all of the tasks from zookeeper and then
     * filer them out.
     *
     * ZK stores tasks by ID, so at the moment there is no more efficient way to search
     * within the storage itself.
     */
    @Override
    public Set<TaskState> getTasks(TaskStatus taskStatus, String taskClassName, String createdBy, int limit, int offset){
        try {

            Stream<TaskState> stream = zookeeper.connection().getChildren()
                    .forPath(TASKS_PATH_PREFIX).stream()
                    .map(this::getState);

            if (taskStatus != null) {
                stream = stream.filter(t -> t.status().equals(taskStatus));
            }

            if (taskClassName != null) {
                stream = stream.filter(t -> t.taskClassName().equals(taskClassName));
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

    private <T> T executeWithMutex(String id, SupplierWithException<T> function){
        InterProcessMutex mutex = zookeeper.mutex(id);

        zookeeper.acquire(mutex);
        try {
            return function.get();
        } catch (Exception e) {
            throw new EngineStorageException("Could not get state from storage " + getFullStackTrace(e));
        } finally {
            zookeeper.release(mutex);
        }
    }

    @FunctionalInterface
    private interface SupplierWithException<T> {
        public T get() throws Exception;
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
    private String taskPath(String id){
        return format(ZK_TASK_PATH, id);
    }

    /**
     * Path representing the task running on an engine
     * @param engineId Identifier of the engine
     * @param taskState Identifier of the task
     * @return Path representing conbination between engine and task
     */
    private String engineTaskPath(String engineId, TaskState taskState){
        return format(ZK_ENGINE_TASK_PATH, engineId, taskState.getId());
    }

    /**
     * Path to a single engine
     * @param engineId Identifier of the engine
     * @return Path to the engine
     */
    private String enginePath(String engineId){
        return format(ZK_ENGINE_PATH, engineId);
    }
}
