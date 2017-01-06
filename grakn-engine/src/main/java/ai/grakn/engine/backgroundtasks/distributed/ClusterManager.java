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
import ai.grakn.engine.util.ExceptionWrapper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;

import java.util.concurrent.*;

import static ai.grakn.engine.backgroundtasks.config.ZookeeperPaths.*;
import static ai.grakn.engine.util.ExceptionWrapper.noThrow;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;

/**
 * Scheduler will be constantly running on the "Leader" machine. The "takeLeadership"
 * function in this class will be called if it is needed to take over.
 */
public class ClusterManager extends LeaderSelectorListenerAdapter {
    private static ClusterManager instance = null;
    private final KafkaLogger LOG = KafkaLogger.getInstance();
    private final String engineID;

    private LeaderSelector leaderSelector;
    private Scheduler scheduler;
    private TreeCache cache;
    private TaskRunner taskRunner;
    private Thread taskRunnerThread;
    private SynchronizedStateStorage zookeeperStorage;
    private CountDownLatch leaderInitLatch = new CountDownLatch(1);
    
    public static synchronized ClusterManager getInstance() {
        if(instance == null)
            instance = new ClusterManager();

        return instance;
    }

    private ClusterManager() {
        this.engineID = EngineID.getInstance().id();
    }

    public void start() {
        try {
            LOG.open();
            LOG.debug("Starting Cluster manager, called by "+Thread.currentThread().getStackTrace()[1]);

            zookeeperStorage = SynchronizedStateStorage.getInstance();

            // Call close() in case there is an exception during open().
            taskRunner = new TaskRunner();//countDownLatch);
            taskRunner.open();
            taskRunnerThread = new Thread(taskRunner);
            taskRunnerThread.start();

            leaderSelector = new LeaderSelector(zookeeperStorage.connection(), SCHEDULER, this);
            leaderSelector.autoRequeue();

            // the selection for this instance doesn't start until the leader selector is started
            // leader selection is done in the background so this call to leaderSelector.start() returns immediately
            leaderSelector.start();            	
            while (!leaderSelector.getLeader().isLeader()) {
                Thread.sleep(1000);
            }
            if (leaderSelector.hasLeadership())
            	leaderInitLatch.await();
        }
        catch (Exception e) {
            LOG.error(getFullStackTrace(e));
            throw new RuntimeException(e);
        }

        LOG.debug("ClusterManager started, a leader has been elected.");
    }

    public void stop() {
        noThrow(leaderSelector::interruptLeadership, "Could not interrupt leadership.");
        noThrow(leaderSelector::close, "Could not close leaderSelector.");
        if(scheduler != null)
            noThrow(scheduler::close, "Could not stop scheduler.");

        if(cache != null)
            noThrow(cache::close, "Could not close ZK Tree Cache.");

        noThrow(taskRunner::close, "Could not stop TaskRunner.");

        // Lambdas cant throw exceptions
        try {
            taskRunnerThread.join();
        } catch(Throwable t) {
            LOG.error("Exception whilst waiting for TaskRunner thread to join - "+getFullStackTrace(t));
        }

        noThrow(zookeeperStorage::close, "Could not close ZK storage.");
        zookeeperStorage = null;

        noThrow(LOG::close, "Could not close KafkaLogger");
    }

    /**
     * When you take over leadership start a new Scheduler instance and wait for it to complete.
     * @throws Exception
     */
    public void takeLeadership(CuratorFramework client) throws Exception {
        registerFailover(client);
        
        // Call close() in case of exceptions during open()
        scheduler = new Scheduler();
        scheduler.open();

        LOG.info(engineID + " has taken over the scheduler.");
        Thread schedulerThread = new Thread(scheduler);
        schedulerThread.setDaemon(true);
        schedulerThread.start();
        leaderInitLatch.countDown();        
        schedulerThread.join();
    }

    /**
     * Get the scheduler object
     * @return scheduler
     */
    public Scheduler getScheduler() {
        return scheduler;
    }

    private void registerFailover(CuratorFramework client) throws Exception {
        cache = new TreeCache(client, RUNNERS_WATCH);
        try(TaskFailover failover = TaskFailover.getInstance().open(client, cache)) {
            cache.getListenable().addListener(failover);
        }

        cache.start();
    }
}
