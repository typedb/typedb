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
import ai.grakn.engine.tasks.manager.distributed.ZookeeperConnection;
import ai.grakn.exception.EngineStorageException;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.engine.tasks.config.ZookeeperPaths.TASKS_PATH_PREFIX;
import static ai.grakn.engine.tasks.config.ZookeeperPaths.TASK_LOCK_SUFFIX;
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
    private static final String ZK_TASK_PATH =  TASKS_PATH_PREFIX + "/%s" + TASK_STATE_SUFFIX;

    private final Logger LOG = LoggerFactory.getLogger(TaskStateZookeeperStore.class);
    private final ZookeeperConnection zookeeper;

    public TaskStateZookeeperStore(ZookeeperConnection zookeeper) {
        this.zookeeper = zookeeper;
    }

    @Override
    public String newState(TaskState task){
        InterProcessMutex mutex = mutex(task.getId());

        acquire(mutex);
        try {
            zookeeper.write(taskPath(task), serialize(task));
        } catch (Exception exception){
            throw new EngineStorageException("Could not write state to storage " + getFullStackTrace(exception));
        } finally {
            release(mutex);
        }

        return task.getId();
    }

    @Override
    public Boolean updateState(TaskState task){
        InterProcessMutex mutex = mutex(task.getId());

        acquire(mutex);
        try {
            zookeeper.put(taskPath(task), serialize(task));
        } catch (Exception e) {
            LOG.error("Could not write to ZooKeeper! - "+e);
            return false;
        } finally {
            release(mutex);
        }

        return true;
    }

    /**
     * Retrieve the TaskState associated with the given ID. Acquire a distributed mutex before executing
     * to ensure most up-to-date state.
     * @param id String id of task.
     * @return State of the given task
     */
    @Override
    public TaskState getState(String id) {
        InterProcessMutex mutex = mutex(id);

        acquire(mutex);
        try {
            return (TaskState) deserialize(zookeeper.read(taskPath(id)));
        } catch (Exception e) {
            throw new EngineStorageException("Could not get state from storage " + getFullStackTrace(e));
        } finally {
            release(mutex);
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

    private InterProcessMutex mutex(String id){
        return new InterProcessMutex(zookeeper.connection(), TASKS_PATH_PREFIX + id + TASK_LOCK_SUFFIX);
    }

    private void acquire(InterProcessMutex mutex){
        try {
            mutex.acquire();
        } catch (Exception e) {
            throw new EngineStorageException("Error acquiring mutex from zookeeper.");
        }
    }

    private void release(InterProcessMutex mutex){
        try {
            mutex.release();
        } catch (Exception e) {
            throw new EngineStorageException("Error releasing mutex from zookeeper.");
        }
    }
}
