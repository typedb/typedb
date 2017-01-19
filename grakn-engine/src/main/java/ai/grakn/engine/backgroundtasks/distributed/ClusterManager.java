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

import ai.grakn.engine.backgroundtasks.taskstorage.SynchronizedStateStorage;
import ai.grakn.engine.util.EngineID;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;

import static ai.grakn.engine.backgroundtasks.config.ZookeeperPaths.RUNNERS_WATCH;
import static ai.grakn.engine.backgroundtasks.config.ZookeeperPaths.SCHEDULER;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;

/**
 *
 * ClusterManager controls the behaviour of the entire Grakn Engine cluster.
 *
 * There is one "Scheduler" that will be constantly running on the "Leader" machine.
 *
 * This class begins the TaskRunner instance that will be running on this machine.
 * This class registers this instance of Engine with Zookeeper & the Leader election process.
 *
 * If this machine is required to take over the Scheduler, the "takeLeadership" function will be called.
 * If this machine controls the Scheduler and leadership is relinquished/lost, the "stateChanged" function is called
 * and the Scheduler is stopped before a error is thrown.
 *
 * The Scheduler thread is a daemon thread
 *
 * @author Denis Lobanov, alexandraorth
 */
public class ClusterManager extends LeaderSelectorListenerAdapter {
    private static final String SCHEDULER_THREAD_NAME = "scheduler-";
    private static final String TASKRUNNER_THREAD_NAME = "taskrunner-";

    private static final KafkaLogger LOG = KafkaLogger.getInstance();
    private static final String ENGINE_ID = EngineID.getInstance().id();

    // Threads in which to run task manager & scheduler
    private Thread taskRunnerThread;
    private Thread schedulerThread;

    private DistributedTaskManager taskManager;
    private SynchronizedStateStorage zookeeperStorage;
    private LeaderSelector leaderSelector;
    private Scheduler scheduler;
    private TreeCache cache;
    private TaskRunner taskRunner;
    private TaskFailover failover;

    public ClusterManager() {
        try {
            LOG.debug("Starting Cluster manager on " + ENGINE_ID);

            startZookeeperConnection();
            electLeader();
            startTaskManager();
            startTaskRunner();
        }
        catch (Exception e) {
            throw new RuntimeException("Could not start ClusterManager on " + ENGINE_ID + getFullStackTrace(e));
        }

        LOG.debug("ClusterManager started & a leader elected on " + ENGINE_ID);
    }

    /**
     * When stopping the ClusterManager we must:
     *  1. Interrupt the Scheduler on whichever machine it is running on. We do this by interrupting the leadership.
     *      When leadership is interrupted it will shut down the Scheduler on that machine.
     *
     *  2. Shutdown the TaskRunner on this machine.
     *
     *  3. Shutdown Zookeeper storage connection on this machine
     */
    public void stop() throws Exception {
        leaderSelector.interruptLeadership();
        leaderSelector.close();

        stopScheduler();
        stopTaskManager();
        stopTaskRunner();
        stopZookeeperConnection();
    }

    /**
     * On leadership takeover, start a new Scheduler instance and wait for it to complete.
     * The method should not return until leadership is relinquished.
     * @throws Exception
     */
    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {
        registerFailover(client);

        // start the scheduler
        startScheduler();

        // wait for scheduler to fail
        schedulerThread.join();
    }

    /**
     * Get the scheduler object that is running on this machine
     */
    public Scheduler getScheduler() {
        return scheduler;
    }

    /**
     * Get the Zookeeper storage connection for this machine
     */
    public SynchronizedStateStorage getStorage(){
        return zookeeperStorage;
    }

    /**
     * Get the task manager for this machine
     */
    public DistributedTaskManager getTaskManager(){
        return taskManager;
    }

    /**
     *
     * This method blocks until the leader has been elected.
     * @throws Exception Any error connecting to Zookeeper while waiting for Leader
     */
    private void electLeader() throws Exception {
        leaderSelector = new LeaderSelector(zookeeperStorage.connection(), SCHEDULER, this);
        leaderSelector.autoRequeue();

        // the selection for this instance doesn't start until the leader selector is started
        // leader selection is done in the background so this call to leaderSelector.start() returns immediately
        leaderSelector.start();
        while (!leaderSelector.getLeader().isLeader()) {
            Thread.sleep(1000);
        }
    }

    /**
     * Instantiate an instance of the distributed task manager to accept and run tasks
     */
    private void startTaskManager() {
        taskManager = new DistributedTaskManager(zookeeperStorage);
    }

    /**
     * Close this instance of the distributed task manager
     */
    private void stopTaskManager(){
        taskManager.close();
    }

    /**
     * Instantiate an instance of task runner and start running it in a thread.
     * @throws Exception If an exception is thrown opening the TaskRunner
     */
    private void startTaskRunner() throws Exception {
        taskRunner = new TaskRunner(zookeeperStorage);

        taskRunnerThread = new Thread(taskRunner, TASKRUNNER_THREAD_NAME + taskRunner.hashCode());
        taskRunnerThread.start();
    }

    /**
     * Close the instance of TaskRunner on this machine and wait for the TaskRunner thread to die
     * @throws InterruptedException If any thread has interrupted the shutting down of the TaskRunner thread
     */
    private void stopTaskRunner() throws InterruptedException {
        taskRunner.close();
        taskRunnerThread.join();
    }

    /**
     * Return the connection to Zookeeper for this machine
     * @throws Exception If there was a problem instantiating connection to Zookeeper
     */
    private void startZookeeperConnection() throws Exception{
        zookeeperStorage = new SynchronizedStateStorage();
    }

    /**
     * Close all connections to Zookeeper
     */
    private void stopZookeeperConnection(){
        cache.close();
        failover.close();
        zookeeperStorage.close();
    }

    /**
     * Instantiate a Scheduler object and start running it in a daemon thread
     * @throws Exception If an exception is thrown opening the Scheduler
     */
    private void startScheduler() throws Exception {
        LOG.info(ENGINE_ID + " has taken over the scheduler");

        scheduler = new Scheduler(zookeeperStorage);

        schedulerThread = new Thread(scheduler, SCHEDULER_THREAD_NAME + scheduler.hashCode());
        schedulerThread.setDaemon(true);
        schedulerThread.start();
    }

    /**
     * Close this instance of the Scheduler.
     */
    private void stopScheduler(){
        scheduler.close();
    }

    private void registerFailover(CuratorFramework client) throws Exception {
        cache = new TreeCache(client, RUNNERS_WATCH);
        failover = new TaskFailover(client, cache, zookeeperStorage);
        cache.getListenable().addListener(failover);
        cache.start();
    }
}
