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

package ai.grakn.engine.backgroundtasks.taskstatestorage;

import ai.grakn.engine.backgroundtasks.TaskStateStorage;
import ai.grakn.engine.backgroundtasks.TaskState;
import ai.grakn.engine.backgroundtasks.TaskStatus;
import ai.grakn.engine.backgroundtasks.distributed.ZookeeperConnection;
import ai.grakn.exception.EngineStorageException;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.engine.backgroundtasks.config.ZookeeperPaths.TASKS_PATH_PREFIX;
import static ai.grakn.engine.backgroundtasks.config.ZookeeperPaths.TASK_STATE_SUFFIX;
import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.lang.SerializationUtils.deserialize;
import static org.apache.commons.lang.SerializationUtils.serialize;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;

/**
 * <p>
 * Manages the state of background {@link ai.grakn.engine.backgroundtasks.BackgroundTask} in
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
    private final CuratorFramework zookeeperConnection;

    public TaskStateZookeeperStore(ZookeeperConnection connection) {
        zookeeperConnection = connection.connection();
    }

    @Override
    public String newState(TaskState task){
        try {
            zookeeperConnection.create()
                    .creatingParentContainersIfNeeded()
                    .forPath(format(ZK_TASK_PATH, task.getId()), serialize(task));

        } catch (Exception exception){
            throw new EngineStorageException("Could not write state to storage " + getFullStackTrace(exception));
        }

        return task.getId();
    }

    @Override
    public Boolean updateState(TaskState task){
        try {
            zookeeperConnection.setData()
                    .forPath(format(ZK_TASK_PATH, task.getId()), serialize(task));
        }
        catch (Exception e) {
            LOG.error("Could not write to ZooKeeper! - "+e);
            return false;
        }

        return true;
    }

    @Override
    public TaskState getState(String id) {
        try {
            byte[] b = zookeeperConnection.getData().forPath(TASKS_PATH_PREFIX+"/"+id+TASK_STATE_SUFFIX);
            return (TaskState) deserialize(b);
        }
        catch (Exception e) {
            throw new EngineStorageException("Could not get state from storage " + getFullStackTrace(e));
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

            Stream<TaskState> stream = zookeeperConnection.getChildren()
                    .forPath(TASKS_PATH_PREFIX).stream()
                    .map(this::getState)
                    .filter(t -> taskStatus != null && t.status().equals(taskStatus))
                    .filter(t -> taskClassName != null && t.taskClassName().equals(taskClassName))
                    .filter(t -> createdBy != null && t.creator().equals(createdBy))
                    .skip(offset);

            if(limit > 0){
                stream = stream.limit(limit);
            }

            return stream.collect(toSet());
        } catch (Exception e){
            throw new EngineStorageException("Could not get state from storage " + getFullStackTrace(e));
        }
    }
}
