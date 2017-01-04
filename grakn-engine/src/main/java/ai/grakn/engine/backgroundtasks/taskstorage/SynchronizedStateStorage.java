/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Ltd
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

import ai.grakn.engine.backgroundtasks.TaskStatus;
import ai.grakn.engine.backgroundtasks.config.ConfigHelper;
import ai.grakn.engine.backgroundtasks.distributed.KafkaLogger;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.TimeUnit;

import static ai.grakn.engine.backgroundtasks.config.ZookeeperPaths.RUNNERS_STATE;
import static ai.grakn.engine.backgroundtasks.config.ZookeeperPaths.RUNNERS_WATCH;
import static ai.grakn.engine.backgroundtasks.config.ZookeeperPaths.SCHEDULER;
import static ai.grakn.engine.backgroundtasks.config.ZookeeperPaths.TASKS_PATH_PREFIX;
import static ai.grakn.engine.backgroundtasks.config.ZookeeperPaths.TASK_STATE_SUFFIX;
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
public class SynchronizedStateStorage {
    private final KafkaLogger LOG = KafkaLogger.getInstance();
    private static SynchronizedStateStorage instance = null;

    private CuratorFramework zookeeperConnection;

    private SynchronizedStateStorage() throws Exception {
        zookeeperConnection = ConfigHelper.client();
        zookeeperConnection.start();
        zookeeperConnection.blockUntilConnected();

        createZKPaths();
    }

    public static synchronized SynchronizedStateStorage getInstance() throws Exception {
        if(instance == null){
            instance = new SynchronizedStateStorage();
        }

        return instance;
    }

    public CuratorFramework connection(){
        return zookeeperConnection;
    }

    public void close() {
        zookeeperConnection.close();
        instance = null;
    }

    public void newState(String id, TaskStatus status, String engineID, String checkpoint) throws Exception {
        if(id == null || status == null)
            return;

        // Serialise to SynchronizedState obj
        SynchronizedState state = new SynchronizedState(status);
        if(engineID != null)
            state.engineID(engineID);
        if(checkpoint != null)
            state.checkpoint(checkpoint);

        zookeeperConnection.create()
              .creatingParentContainersIfNeeded()
              .forPath(TASKS_PATH_PREFIX+"/"+id+TASK_STATE_SUFFIX, state.serialize().getBytes());
    }

    public Boolean updateState(String id, TaskStatus status, String engineID, String checkpoint) {
        if(id == null)
            return false;

        if(status == null && engineID == null && checkpoint == null)
            return false;

        try {
            SynchronizedState state = getState(id);
            if(state == null)
                return false;

            // Update values
            if (status != null)
                state.status(status);
            if (engineID != null)
                state.engineID(engineID);
            if (checkpoint != null)
                state.checkpoint(checkpoint);

            // Save to ZK
            zookeeperConnection.setData().forPath(TASKS_PATH_PREFIX+"/"+id+TASK_STATE_SUFFIX, state.serialize().getBytes());
        }
        catch (Exception e) {
            LOG.error("Could not write to ZooKeeper! - "+e);
            return false;
        }

        return true;
    }

    public SynchronizedState getState(String id) {
        try {
            byte[] b = zookeeperConnection.getData().forPath(TASKS_PATH_PREFIX+"/"+id+TASK_STATE_SUFFIX);
            return SynchronizedState.deserialize(new String(b));
        }
        catch (Exception e) {
            LOG.error(" Could not read from ZooKeeper! " + getFullStackTrace(e));
        }

        return null;
    }

    private void createZKPaths() throws Exception {
        if(zookeeperConnection.checkExists().forPath(SCHEDULER) == null)
            zookeeperConnection.create().creatingParentContainersIfNeeded().forPath(SCHEDULER);

        if(zookeeperConnection.checkExists().forPath(RUNNERS_WATCH) == null)
            zookeeperConnection.create().creatingParentContainersIfNeeded().forPath(RUNNERS_WATCH);

        if(zookeeperConnection.checkExists().forPath(RUNNERS_STATE) == null)
            zookeeperConnection.create().creatingParentContainersIfNeeded().forPath(RUNNERS_STATE);

        if(zookeeperConnection.checkExists().forPath(TASKS_PATH_PREFIX) == null)
            zookeeperConnection.create().creatingParentContainersIfNeeded().forPath(TASKS_PATH_PREFIX);
    }
}
