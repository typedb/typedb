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

package ai.grakn.engine.tasks.manager.singlequeue;

import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.engine.tasks.manager.ZookeeperConnection;
import ai.grakn.engine.tasks.manager.multiqueue.TaskFailover;
import ai.grakn.engine.util.EngineID;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.CancelLeadershipException;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.state.ConnectionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static ai.grakn.engine.tasks.config.ZookeeperPaths.FAILOVER;
import static ai.grakn.engine.util.ExceptionWrapper.noThrow;

/**
 * The {@link FailoverElector} it registers itself with the leadership election and controls the lifecycle of
 * {@link TaskFailover} if elected
 *
 * @author alexandraorth
 */
public class FailoverElector extends LeaderSelectorListenerAdapter {

    private final Logger LOG = LoggerFactory.getLogger(FailoverElector.class);
    private final LeaderSelector leaderSelector;
    private final EngineID identifier;
    private final TaskStateStorage storage;
    private TaskFailover failover;

    /**
     * Instantiating a {@link FailoverElector} adds this engine to the leadership selection process
     */
    public FailoverElector(EngineID identifier, ZookeeperConnection zookeeper, TaskStateStorage storage){
        this.identifier = identifier;
        this.storage = storage;

        leaderSelector = new LeaderSelector(zookeeper.connection(), FAILOVER, this);
        leaderSelector.setId(identifier.value());
        leaderSelector.autoRequeue();

        // the selection for this instance doesn't start until the leader selector is started
        // leader selection is done in the background so this call to leaderSelector.start() returns immediately
        leaderSelector.start();
        awaitLeader();
    }

    /**
     *
     */
    public void renounce(){
        noThrow(leaderSelector::interruptLeadership, "Error interrupting leadership");
        noThrow(leaderSelector::close, "Error closing leadership elector");
    }

    /**
     * Return the identifier of the current leader. Block until there is a leader.
     */
    public String awaitLeader(){
        try {
            while (!leaderSelector.getLeader().isLeader()) {
                Thread.sleep(1000);
            }
            return leaderSelector.getLeader().getId();
        } catch (Exception e){
            throw new RuntimeException("There were errors electing a leader", e);
        }
    }

    /**
     * Called when this instance of TaskFailover is elected as leader and starts listening for
     * failed engines.
     */
    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {
        LOG.debug("Leadership taken by: " + identifier);

        failover = new TaskFailover(client, storage);
        failover.await();
    }

    /**
     * Called when this instance of {@link FailoverElector} is leader and the state has changes in any way.
     * If leadership is suspended or lost the {@link TaskFailover} should be closed.
     */
    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState)
    {
        switch (newState) {
            case LOST:
                noThrow(failover::close, "Error closing task failover");
                throw new CancelLeadershipException();
            case CONNECTED:
                break;
            case SUSPENDED:
                LOG.debug("Leadership suspended");
                break;
            case RECONNECTED:
                LOG.debug("Leadership regained");
                break;
            default:
                break;
        }
    }
}
