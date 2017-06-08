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
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.GraknEngineServer;
import ai.grakn.engine.tasks.TaskManager;
import ai.grakn.engine.tasks.connection.RedisConnection;
import ai.grakn.engine.tasks.manager.StandaloneTaskManager;
import ai.grakn.engine.tasks.manager.singlequeue.SingleQueueTaskManager;
import ai.grakn.engine.tasks.mock.MockBackgroundTask;
import org.junit.rules.ExternalResource;
import com.jayway.restassured.RestAssured;
import javax.annotation.Nullable;
import static ai.grakn.engine.GraknEngineConfig.REDIS_SERVER_PORT;
import static ai.grakn.engine.GraknEngineConfig.REDIS_SERVER_URL;
import static ai.grakn.engine.GraknEngineConfig.TASK_MANAGER_IMPLEMENTATION;
import static ai.grakn.engine.util.ExceptionWrapper.noThrow;
import static ai.grakn.test.GraknTestEnv.randomKeyspace;
import static ai.grakn.test.GraknTestEnv.startEngine;
import static ai.grakn.test.GraknTestEnv.startKafka;
import static ai.grakn.test.GraknTestEnv.startRedis;
import static ai.grakn.test.GraknTestEnv.stopEngine;
import static ai.grakn.test.GraknTestEnv.stopRedis;

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
    private final boolean startSingleQueueEngine;
    private final boolean startStandaloneEngine;
    private final GraknEngineConfig config = GraknEngineConfig.create();

    private EngineContext(boolean startKafka, boolean startSingleQueueEngine, boolean startStandaloneEngine){
        this.startSingleQueueEngine = startSingleQueueEngine;
        this.startStandaloneEngine = startStandaloneEngine;
        this.startKafka = startKafka;
    }

    public static EngineContext startKafkaServer(){
        return new EngineContext(true, false, false);
    }

    public static EngineContext startSingleQueueServer(){
        return new EngineContext(true, true, false);
    }

    public static EngineContext startInMemoryServer(){
        return new EngineContext(false, false, true);
    }

    public EngineContext port(int port) {
        config.setConfigProperty(GraknEngineConfig.SERVER_PORT_NUMBER, String.valueOf(port));
        return this;
    }

    public GraknEngineServer server() {
        return server;
    }

    public GraknEngineConfig config() {
        return config;
    }

    public RedisConnection redis() {
        return RedisConnection.create(config.getProperty(REDIS_SERVER_URL), config.getPropertyAsInt(REDIS_SERVER_PORT));
    }

    public TaskManager getTaskManager(){
        return server.getTaskManager();
    }

    //TODO Rename this method to "sessionWithNewKeyspace"
    public GraknSession factoryWithNewKeyspace() {
        return Grakn.session(GraknTestEnv.getUri(config), randomKeyspace());
    }

    @Override
    public void before() throws Throwable {
        RestAssured.baseURI = "http://" + config.getProperty("server.host") + ":" + config.getProperty("server.port");        
        if (!config.getPropertyAsBool("test.start.embedded.components", true)) {
            return;
        }
        if(startKafka){
            startKafka(config);
        }

        startRedis(config);

        @Nullable Class<? extends TaskManager> taskManagerClass = null;

        if(startSingleQueueEngine){
            taskManagerClass = SingleQueueTaskManager.class;
        }

        if (startStandaloneEngine){
            taskManagerClass = StandaloneTaskManager.class;
        }

        if (taskManagerClass != null) {
            config.setConfigProperty(TASK_MANAGER_IMPLEMENTATION, taskManagerClass.getName());
            server = startEngine(config);
        }
    }

    @Override
    public void after() {
        if (!config.getPropertyAsBool("test.start.embedded.components", true)) {
            return;
        }
        noThrow(MockBackgroundTask::clearTasks, "Error clearing tasks");

        try {
            if(startSingleQueueEngine | startStandaloneEngine){
                noThrow(() -> stopEngine(server), "Error closing engine");
            }

            if(startKafka){
                noThrow(GraknTestEnv::stopKafka, "Error stopping kafka");
            }

            stopRedis();
        } catch (Exception e){
            throw new RuntimeException("Could not shut down ", e);
        }
    }
}
