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
import ai.grakn.GraknSession;
import ai.grakn.engine.GraknEngineServer;
import ai.grakn.engine.tasks.TaskManager;
import ai.grakn.engine.tasks.manager.StandaloneTaskManager;
import ai.grakn.engine.tasks.manager.multiqueue.MultiQueueTaskManager;
import ai.grakn.engine.tasks.manager.singlequeue.SingleQueueTaskManager;
import ai.grakn.engine.tasks.mock.MockBackgroundTask;
import org.junit.rules.ExternalResource;

import static ai.grakn.engine.util.ExceptionWrapper.noThrow;
import static ai.grakn.test.GraknTestEnv.randomKeyspace;
import static ai.grakn.test.GraknTestEnv.startEngine;
import static ai.grakn.test.GraknTestEnv.startKafka;
import static ai.grakn.test.GraknTestEnv.stopEngine;

/**
 * <p>
 * Start the Grakn Engine server before each test class and stop after.
 * </p>
 *
 * @author alexandraorth
 */
public class EngineContext extends ExternalResource {

    private GraknEngineServer server;

    private final boolean startKafka;
    private final boolean startMultiQueueEngine;
    private final boolean startSingleQueueEngine;
    private final boolean startStandaloneEngine;
    private int port = 4567;

    private EngineContext(boolean startKafka, boolean startMultiQueueEngine, boolean startSingleQueueEngine, boolean startStandaloneEngine){
        this.startMultiQueueEngine = startMultiQueueEngine;
        this.startSingleQueueEngine = startSingleQueueEngine;
        this.startStandaloneEngine = startStandaloneEngine;
        this.startKafka = startKafka;
    }

    public static EngineContext startKafkaServer(){
        return new EngineContext(true, false, false, false);
    }

    public static EngineContext startMultiQueueServer(){
        return new EngineContext(true, true, false, false);
    }

    public static EngineContext startSingleQueueServer(){
        return new EngineContext(true, false, true, false);
    }

    public static EngineContext startInMemoryServer(){
        return new EngineContext(false, false, false, true);
    }

    public EngineContext port(int port) {
        this.port = port;
        return this;
    }

    public GraknEngineServer server() {
        return server;
    }

    public TaskManager getTaskManager(){
        return server.getTaskManager();
    }

    public GraknSession factoryWithNewKeyspace() {
        return Grakn.session("localhost:" + port, randomKeyspace());
    }

    @Override
    public void before() throws Throwable {
        if(startKafka){
            startKafka();
        }

        if(startSingleQueueEngine){
            server = startEngine(SingleQueueTaskManager.class.getName(), port);
        }

        if (startMultiQueueEngine){
            //TODO: This patch should die when we get rid of MultiQueueTaskManager
            if(startSingleQueueEngine) server = startEngine(MultiQueueTaskManager.class.getName(), port);
        }

        if (startStandaloneEngine){
            server = startEngine(StandaloneTaskManager.class.getName(), port);
        }
    }

    @Override
    public void after() {
        noThrow(MockBackgroundTask::clearTasks, "Error clearing tasks");

        try {
            if(startMultiQueueEngine | startSingleQueueEngine | startStandaloneEngine){
                noThrow(() -> stopEngine(server), "Error closing engine");
            }

            if(startKafka){
                noThrow(GraknTestEnv::stopKafka, "Error stopping kafka");
            }
        } catch (Exception e){
            throw new RuntimeException("Could not shut down ", e);
        }
    }
}
