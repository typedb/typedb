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

import ai.grakn.engine.tasks.manager.ZookeeperConnection;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;

import static ai.grakn.engine.util.ExceptionWrapper.noThrow;

/**
 *
 * @author alexandraorth
 */
public class FailoverElector extends LeaderSelectorListenerAdapter implements TreeCacheListener {

//    private final LeaderSelector leaderSelector;

    /**
     *
     */
    public FailoverElector(ZookeeperConnection zookeeper){

    }

    /**
     *
     */
    public void stop(){

    }

    /**
     * Called when this instance of TaskFailover is elected as leader and starts listening for
     * failed engines.
     *
     * @param client
     * @throws Exception
     */
    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {

    }

    /**
     * Triggered when the status of an engine changes.
     * @param client
     * @param event
     * @throws Exception
     */
    @Override
    public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {

    }
}
