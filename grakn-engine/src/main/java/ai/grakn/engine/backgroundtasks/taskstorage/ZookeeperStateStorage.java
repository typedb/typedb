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

import ai.grakn.engine.backgroundtasks.StateStorage;
import ai.grakn.engine.backgroundtasks.TaskState;
import ai.grakn.engine.backgroundtasks.TaskStatus;
import ai.grakn.engine.backgroundtasks.config.ConfigHelper;
import ai.grakn.engine.backgroundtasks.distributed.KafkaLogger;
import javafx.util.Pair;
import org.apache.curator.framework.CuratorFramework;
import org.json.JSONObject;

import java.time.Instant;
import java.util.Set;

import static ai.grakn.engine.backgroundtasks.config.ZookeeperPaths.RUNNERS_STATE;
import static ai.grakn.engine.backgroundtasks.config.ZookeeperPaths.RUNNERS_WATCH;
import static ai.grakn.engine.backgroundtasks.config.ZookeeperPaths.SCHEDULER;
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
public class ZookeeperStateStorage implements StateStorage {
    private static final String ZK_TASK_PATH =  TASKS_PATH_PREFIX + "/%s" + TASK_STATE_SUFFIX;

    private final KafkaLogger LOG = KafkaLogger.getInstance();
    private final CuratorFramework zookeeperConnection = ConfigHelper.client();

    public ZookeeperStateStorage() throws Exception {
        zookeeperConnection.start();
        zookeeperConnection.blockUntilConnected();

        createZKPaths();
    }

    public void close() {
        zookeeperConnection.close();
    }

    public CuratorFramework connection(){
        return zookeeperConnection;
    }

    @Override
    public String newState(String taskName, String createdBy, Instant runAt, Boolean recurring, long interval, JSONObject configuration) {
        if (taskName == null || createdBy == null || runAt == null || recurring == null) {
            return null;
        }

        TaskState task = new TaskState(taskName)
                .status(TaskStatus.CREATED)
                .creator(createdBy)
                .runAt(runAt)
                .isRecurring(recurring)
                .interval(interval)
                .configuration(configuration);

        try {
            zookeeperConnection.create()
                    .creatingParentContainersIfNeeded()
                    .forPath(format(ZK_TASK_PATH, task.getId()), task.serialise());

        } catch (Exception exception){
            LOG.error("Could not write task state to Zookeeper");
            throw new RuntimeException("Could not write state to storage " + getFullStackTrace(exception));
        }

        return task.getId();
    }

    @Override
    public Boolean updateState(String id, TaskStatus status, String statusChangeBy, String engineID, Throwable failure, String checkpoint, JSONObject configuration){
        if(id == null) {
            return false;
        }

        if(status == null && engineID == null && checkpoint == null) {
            return false;
        }

       TaskState task = getState(id);
        if(task == null) {
            return false;
        }

        // Update values
        if (status != null) {
            task.status(status);
        }
        if (engineID != null) {
            task.engineID(engineID);
        }
        if (checkpoint != null) {
            task.checkpoint(checkpoint);
        }

        try {
            // Save to ZK
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

    private void createZKPaths() throws Exception {
        if(zookeeperConnection.checkExists().forPath(SCHEDULER) == null) {
            zookeeperConnection.create().creatingParentContainersIfNeeded().forPath(SCHEDULER);
        }

        if(zookeeperConnection.checkExists().forPath(RUNNERS_WATCH) == null) {
            zookeeperConnection.create().creatingParentContainersIfNeeded().forPath(RUNNERS_WATCH);
        }

        if(zookeeperConnection.checkExists().forPath(RUNNERS_STATE) == null) {
            zookeeperConnection.create().creatingParentContainersIfNeeded().forPath(RUNNERS_STATE);
        }

        if(zookeeperConnection.checkExists().forPath(TASKS_PATH_PREFIX) == null) {
            zookeeperConnection.create().creatingParentContainersIfNeeded().forPath(TASKS_PATH_PREFIX);
        }
    }
}
