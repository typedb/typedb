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

package ai.grakn.engine.backgroundtasks.distributed;

import ai.grakn.engine.backgroundtasks.config.ConfigHelper;
import ai.grakn.exception.EngineStorageException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;

import java.util.concurrent.TimeUnit;

import static ai.grakn.engine.backgroundtasks.config.ZookeeperPaths.RUNNERS_STATE;
import static ai.grakn.engine.backgroundtasks.config.ZookeeperPaths.RUNNERS_WATCH;
import static ai.grakn.engine.backgroundtasks.config.ZookeeperPaths.SCHEDULER;
import static ai.grakn.engine.backgroundtasks.config.ZookeeperPaths.TASKS_PATH_PREFIX;

/**
 * <p>
 * Task encapsulating the connection to Zookeeper. There should only be one of these instantiated per engine.
 * </p>
 *
 * @author alexandraorth
 */
public class ZookeeperConnection {

    private final CuratorFramework zookeeperConnection = ConfigHelper.client();

    /**
     * Start the connection to zookeeper. This method is blocking.
     */
    public ZookeeperConnection() {
        try {
            zookeeperConnection.start();
            if(!zookeeperConnection.blockUntilConnected(30, TimeUnit.SECONDS)){
                throw new RuntimeException("Could not connect to zookeeper");
            }

            createZKPaths();
        } catch (Exception exception) {
            throw new RuntimeException("Could not connect to zookeeper");
        }
    }

    /**
     * Close the connection to zookeeper. This method is blocking.
     */
    public void close(){
        zookeeperConnection.close();
        boolean notStopped = true;
        while(notStopped){
            if (zookeeperConnection.getState() == CuratorFrameworkState.STOPPED) {
                notStopped = false;
            }
        }
    }

    /**
     * Get the connection to zookeeper
     */
    public CuratorFramework connection(){
        return zookeeperConnection;
    }

    /**
     * Delete any records at the given path
     * @param path
     */
    public void delete(String path) {
        try {
            zookeeperConnection.delete().forPath(path);
        } catch (Exception e){
            throw new EngineStorageException(e);
        }
    }

    /**
     *
     * @param path
     * @param toWrite
     */
    public void write(String path, byte[] toWrite) {
        try {
            zookeeperConnection.create()
                    .creatingParentContainersIfNeeded()
                    .forPath(path, toWrite);
        } catch (Exception e){
            throw new EngineStorageException(e);
        }
    }

    /**
     *
     * @param path
     * @param toWrite
     */
    public void put(String path, byte[] toWrite) {
        try {
            zookeeperConnection.setData().forPath(path, toWrite);
        } catch (Exception e){
            throw new EngineStorageException(e);
        }
    }

    /**
     * Read the data at the given path
     * @param path
     * @return
     */
    public byte[] read(String path){
        try {
            return zookeeperConnection.getData().forPath(path);
        } catch (Exception e){
            throw new EngineStorageException(e);
        }
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
