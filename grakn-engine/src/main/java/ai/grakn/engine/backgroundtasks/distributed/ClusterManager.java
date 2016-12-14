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

package ai.grakn.engine.backgroundtasks.distributed;

import ai.grakn.engine.backgroundtasks.taskstorage.SynchronizedStateStorage;
import ai.grakn.engine.util.EngineID;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;

import java.util.concurrent.*;

import static ai.grakn.engine.backgroundtasks.config.ZookeeperPaths.*;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;

/**
 * Scheduler will be constantly running on the "Leader" machine. The "takeLeadership"
 * function in this class will be called if it is needed to take over.
 */
public class ClusterManager extends LeaderSelectorListenerAdapter {
    private static ClusterManager instance = null;
    private final KafkaLogger LOG = KafkaLogger.getInstance();

    private final String engineID;
    private final CountDownLatch countDownLatch;

    private LeaderSelector leaderSelector;
    private ExecutorService executor;
    private Scheduler scheduler;
    private TreeCache cache;
    private TaskRunner taskRunner;
    private SynchronizedStateStorage zookeeperStorage;

    public static synchronized ClusterManager getInstance() {
        if(instance == null)
            instance = new ClusterManager();

        return instance;
    }

    private ClusterManager() {
        this.engineID = EngineID.getInstance().id();
        // TaskRunner
        countDownLatch = new CountDownLatch(1);
    }

    public void start() {
        executor = Executors.newFixedThreadPool(2);
        zookeeperStorage = SynchronizedStateStorage.getInstance();

        try {
            taskRunner = new TaskRunner(countDownLatch);
            executor.submit(taskRunner);

            leaderSelector = new LeaderSelector(zookeeperStorage.connection(), SCHEDULER, this);
            leaderSelector.autoRequeue();

            // the selection for this instance doesn't start until the leader selector is started
            // leader selection is done in the background so this call to leaderSelector.start() returns immediately
            leaderSelector.start();
            while (!leaderSelector.getLeader().isLeader()) {
                Thread.sleep(1000);
            }

            // Wait for TaskRunner to start
            countDownLatch.await();
        }
        catch (Exception e) {
            LOG.error(getFullStackTrace(e));
            throw new RuntimeException(e);
        }

        LOG.debug("Leader has been elected");
    }

    public void stop() {
        taskRunner.close();
        if(cache != null)
            cache.close();
        if(scheduler != null)
            scheduler.close();

        leaderSelector.close();
        zookeeperStorage.close();

        executor.shutdown();
        LOG.debug("Cluster Manager stopped");
    }

    /**
     * When you take over leadership start a new Scheduler instance and wait for it to complete.
     * @throws Exception
     */
    public void takeLeadership(CuratorFramework client) throws Exception {
        registerFailover(client);

        scheduler = new Scheduler();
        LOG.info(engineID + " has taken over the scheduler");

        waitOnProcess(executor.submit(scheduler));
        scheduler = null;
    }

    /**
     * Get the scheduler object
     * @return scheduler
     */
    public Scheduler getScheduler() {
        return scheduler;
    }

    //TODO Also wait on register failover
    /**
     * Wait for the scheduler to finish
     * @param future future to wait on
     */
    private void waitOnProcess(Future future) {
        try {
            future.get();
        }
        catch (InterruptedException | ExecutionException e) {
            LOG.error("Scheduler has died " + getFullStackTrace(e));
            Thread.currentThread().interrupt();
        }
        finally {
            leaderSelector.interruptLeadership();
        }
    }

    private void registerFailover(CuratorFramework client) throws Exception {
        cache = new TreeCache(client, RUNNERS_WATCH);
        cache.getListenable().addListener(new TaskFailover(client, cache));
        cache.start();
    }
}
