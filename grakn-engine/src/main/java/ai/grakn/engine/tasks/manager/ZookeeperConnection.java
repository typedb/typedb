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
 *
 */

package ai.grakn.engine.tasks.manager;

import ai.grakn.engine.tasks.config.ZookeeperPaths;
import ai.grakn.engine.GraknEngineConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static ai.grakn.engine.tasks.config.ZookeeperPaths.FAILOVER;
import static ai.grakn.engine.tasks.config.ZookeeperPaths.SCHEDULER;
import static ai.grakn.engine.tasks.config.ZookeeperPaths.TASKS_PATH_PREFIX;
import static ai.grakn.engine.GraknEngineConfig.ZK_BACKOFF_BASE_SLEEP_TIME;
import static ai.grakn.engine.GraknEngineConfig.ZK_BACKOFF_MAX_RETRIES;
import static ai.grakn.engine.GraknEngineConfig.ZK_CONNECTION_TIMEOUT;
import static ai.grakn.engine.GraknEngineConfig.ZK_SERVERS;
import static ai.grakn.engine.GraknEngineConfig.ZK_SESSION_TIMEOUT;

/**
 * <p>
 * Task encapsulating the connection to Zookeeper. There should only be one of these instantiated per engine.
 * </p>
 *
 * @author alexandraorth
 */
public class ZookeeperConnection {

    private static final int ZOOKEEPER_CONNECTION_TIMEOUT = GraknEngineConfig.getInstance().getPropertyAsInt(ZK_CONNECTION_TIMEOUT);
    private static CuratorFramework zookeeperConnection;

    private static final AtomicInteger CONNECTION_COUNTER = new AtomicInteger(0);

    /**
     * Start the connection to zookeeper. This method is blocking.
     */
    public ZookeeperConnection() {
        openClient();

        try {
            createZKPaths();
        } catch (Exception exception) {
            throw new RuntimeException("Could not connect to zookeeper");
        }
    }

    /**
     * Close the connection to zookeeper. This method is blocking.
     */
    public void close(){
        closeClient();
    }

    /**
     * Get the connection to zookeeper
     */
    public CuratorFramework connection(){
        return zookeeperConnection;
    }

    private void createZKPaths() throws Exception {
        if(zookeeperConnection.checkExists().forPath(SCHEDULER) == null) {
            zookeeperConnection.create().creatingParentContainersIfNeeded().forPath(SCHEDULER);
        }

        if(zookeeperConnection.checkExists().forPath(FAILOVER) == null) {
            zookeeperConnection.create().creatingParentContainersIfNeeded().forPath(FAILOVER);
        }

        if(zookeeperConnection.checkExists().forPath(TASKS_PATH_PREFIX) == null) {
            zookeeperConnection.create().creatingParentContainersIfNeeded().forPath(TASKS_PATH_PREFIX);
        }
    }

    private static void openClient() {
        if (CONNECTION_COUNTER.getAndIncrement() == 0) {
            int sleep = GraknEngineConfig.getInstance().getPropertyAsInt(ZK_BACKOFF_BASE_SLEEP_TIME);
            int retries = GraknEngineConfig.getInstance().getPropertyAsInt(ZK_BACKOFF_MAX_RETRIES);

            zookeeperConnection = CuratorFrameworkFactory.builder()
                    .connectString(GraknEngineConfig.getInstance().getProperty(ZK_SERVERS))
                    .namespace(ZookeeperPaths.TASKS_NAMESPACE)
                    .sessionTimeoutMs(GraknEngineConfig.getInstance().getPropertyAsInt(ZK_SESSION_TIMEOUT))
                    .connectionTimeoutMs(GraknEngineConfig.getInstance().getPropertyAsInt(ZK_CONNECTION_TIMEOUT))
                    .retryPolicy(new ExponentialBackoffRetry(sleep, retries))
                    .build();

            zookeeperConnection.start();

            try {
                if(!zookeeperConnection.blockUntilConnected(ZOOKEEPER_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)){
                    throw new RuntimeException("Could not connect to zookeeper");
                }
            } catch (InterruptedException e) {
                throw new RuntimeException("Could not connect to zookeeper");
            }
        }
    }

    private static void closeClient() {
        if (CONNECTION_COUNTER.decrementAndGet() == 0) {
            zookeeperConnection.close();
            boolean notStopped = true;
            while (notStopped) {
                if (zookeeperConnection.getState() == CuratorFrameworkState.STOPPED) {
                    notStopped = false;
                }
            }
        }
    }
}
