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

import ai.grakn.engine.backgroundtasks.TaskStateStorage;
import ai.grakn.engine.backgroundtasks.TaskState;
import ai.grakn.engine.backgroundtasks.TaskStatus;
import ai.grakn.engine.backgroundtasks.distributed.KafkaLogger;
import ai.grakn.engine.backgroundtasks.distributed.ZookeeperConnection;
import javafx.util.Pair;
import org.apache.curator.framework.CuratorFramework;

import java.util.Set;

import static ai.grakn.engine.backgroundtasks.config.ZookeeperPaths.TASKS_PATH_PREFIX;
import static ai.grakn.engine.backgroundtasks.config.ZookeeperPaths.TASK_STATE_SUFFIX;
import static java.lang.String.format;
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
public class ZookeeperStateStorage implements TaskStateStorage {
    private static final String ZK_TASK_PATH =  TASKS_PATH_PREFIX + "/%s" + TASK_STATE_SUFFIX;

    private final KafkaLogger LOG = KafkaLogger.getInstance();
    private final CuratorFramework zookeeperConnection;

    public ZookeeperStateStorage(ZookeeperConnection connection) throws Exception {
        zookeeperConnection = connection.connection();
    }

    @Override
    public String newState(TaskState task){
        try {
            zookeeperConnection.create()
                    .creatingParentContainersIfNeeded()
                    .forPath(format(ZK_TASK_PATH, task.getId()), task.serialise());

        } catch (Exception exception){
            LOG.error("Could not write task state to Zookeeper");
            //TODO do not throw runtime exception
            throw new RuntimeException("Could not write state to storage " + getFullStackTrace(exception));
        }

        return task.getId();
    }

    @Override
    public Boolean updateState(TaskState task){
        try {
            zookeeperConnection.setData()
                    .forPath(format(ZK_TASK_PATH, task.getId()), task.serialise());
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
            return TaskState.deserialise(b);
        }
        catch (Exception e) {
            //TODO do not throw runtime exception
            throw new RuntimeException("Could not get state from storage " + getFullStackTrace(e));
        }
    }

    @Override
    public Set<Pair<String, TaskState>> getTasks(TaskStatus taskStatus, String taskClassName, String createdBy, int limit, int offset){
        throw new UnsupportedOperationException("Task retrieval not supported");
    }
}
