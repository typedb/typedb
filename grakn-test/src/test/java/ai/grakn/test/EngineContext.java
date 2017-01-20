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

package ai.grakn.test;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknGraphFactory;
import ai.grakn.engine.GraknEngineServer;
import ai.grakn.engine.backgroundtasks.distributed.ClusterManager;
import ai.grakn.engine.backgroundtasks.distributed.DistributedTaskManager;
import ai.grakn.engine.util.ConfigProperties;
import org.junit.rules.ExternalResource;

import static ai.grakn.engine.util.ConfigProperties.TASK_MANAGER_INSTANCE;
import static ai.grakn.test.GraknTestEnv.hideLogs;
import static ai.grakn.test.GraknTestEnv.randomKeyspace;
import static ai.grakn.test.GraknTestEnv.startEngine;
import static ai.grakn.test.GraknTestEnv.stopEngine;

/**
 * <p>
 * Start the Grakn Engine server before each test class and stop after.
 * </p>
 *
 * @author alexandraorth
 */
public class EngineContext extends ExternalResource {

    public static EngineContext startServer(){
        return new EngineContext();
    }

    public GraknGraph graphWithNewKeyspace(){
        return factoryWithNewKeyspace().getGraph();
    }

    public ClusterManager getClusterManager(){
        return GraknEngineServer.getClusterManager();
    }

    public GraknGraphFactory factoryWithNewKeyspace() {
        return Grakn.factory(Grakn.DEFAULT_URI, randomKeyspace());
    }

    @Override
    protected void before() throws Throwable {
        //TODO remove when Bug #12029 fixed
        ConfigProperties.getInstance().setConfigProperty(TASK_MANAGER_INSTANCE, DistributedTaskManager.class.getName());

        hideLogs();
        startEngine();
    }

    @Override
    protected void after() {
        try {
            stopEngine();
        } catch (Exception e) {
            e.printStackTrace(System.err);
            throw new RuntimeException("Stopping Engine for test", e);
        }
    }
}
