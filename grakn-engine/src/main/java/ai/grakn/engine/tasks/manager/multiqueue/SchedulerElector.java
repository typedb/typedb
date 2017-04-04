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

package ai.grakn.engine.tasks.manager.multiqueue;

import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.engine.tasks.manager.ZookeeperConnection;
import ai.grakn.engine.tasks.manager.singlequeue.TaskFailover;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.CancelLeadershipException;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.state.ConnectionState;

import static ai.grakn.engine.tasks.config.ConfigHelper.kafkaProducer;
import static ai.grakn.engine.tasks.config.ZookeeperPaths.SCHEDULER;
import static ai.grakn.engine.util.ExceptionWrapper.noThrow;

/**
 * There is one "Scheduler" that will be constantly running on the "Leader" machine.
 *
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
public class SchedulerElector extends LeaderSelectorListenerAdapter {
    private static final String SCHEDULER_THREAD_NAME = "scheduler-";

    private final LeaderSelector leaderSelector;
    private final TaskStateStorage storage;

    private Scheduler scheduler;
    private TaskFailover failover;
    private ZookeeperConnection connection;

    public SchedulerElector(TaskStateStorage storage, ZookeeperConnection connection) {
        this.storage = storage;
        this.connection = connection;

        leaderSelector = new LeaderSelector(connection.connection(), SCHEDULER, this);
        leaderSelector.autoRequeue();

        // the selection for this instance doesn't start until the leader selector is started
        // leader selection is done in the background so this call to leaderSelector.start() returns immediately
        try {
            leaderSelector.start();
            while (!leaderSelector.getLeader().isLeader()) {
                Thread.sleep(1000);
            }
        } catch (Exception e){
            throw new RuntimeException("There were errors electing a leader- Engine should stop");
        }
    }

    /**
     * When stopping the ClusterManager we must:
     *  1. Interrupt the Scheduler on whichever machine it is running on. We do this by interrupting the leadership.
     *      When leadership is interrupted it will shut down the Scheduler on that machine.
     *
     *  2. If the scheduler is running on this machine, close it
     *
     * noThrow() functions used here so that if an error occurs during execution of a
     * certain step, the subsequent stops continue to execute.
     */
    public void stop(){
        leaderSelector.interruptLeadership();
        noThrow(leaderSelector::close, "Error closing leadership elector");

        if(scheduler != null) {
            noThrow(scheduler::close, "Error closing the Scheduler");
            noThrow(failover::close, "Error shutting down task failover hook");
        }
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState)
    {
        if ( (newState == ConnectionState.SUSPENDED) || (newState == ConnectionState.LOST) ) {
            if(scheduler != null) {
                noThrow(scheduler::close, "Error closing the Scheduler");
            }
            throw new CancelLeadershipException();
        }
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
        scheduler = new Scheduler(storage, connection);

        Thread schedulerThread = new Thread(scheduler, SCHEDULER_THREAD_NAME + scheduler.hashCode());
        schedulerThread.start();

        // wait for scheduler to fail
        schedulerThread.join();
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

    private void registerFailover(CuratorFramework client) throws Exception {
        failover = new TaskFailover(client, storage, kafkaProducer());
    }
}
