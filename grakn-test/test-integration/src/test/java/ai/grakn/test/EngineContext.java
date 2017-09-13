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
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.engine.GraknEngineConfig;
import static ai.grakn.engine.GraknEngineConfig.REDIS_HOST;
import static ai.grakn.engine.GraknEngineConfig.TASK_MANAGER_IMPLEMENTATION;
import ai.grakn.engine.GraknEngineServer;
import ai.grakn.engine.SystemKeyspace;
import ai.grakn.engine.tasks.manager.StandaloneTaskManager;
import ai.grakn.engine.tasks.manager.TaskManager;
import ai.grakn.engine.tasks.manager.redisqueue.RedisTaskManager;
import ai.grakn.engine.tasks.mock.MockBackgroundTask;
import static ai.grakn.engine.util.ExceptionWrapper.noThrow;
import ai.grakn.engine.util.SimpleURI;
import static ai.grakn.graql.Graql.var;
import ai.grakn.util.EmbeddedRedis;
import static ai.grakn.util.SampleKBLoader.randomKeyspace;
import com.jayway.restassured.RestAssured;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <p>
 * Start the Grakn Engine server before each test class and stop after.
 * </p>
 *
 * @author alexandraorth
 */
public class EngineContext extends ExternalResource {
    private static final Logger LOG = LoggerFactory.getLogger(EngineContext.class);


    private final GraknEngineServer server;
    private final boolean startSingleQueueEngine;
    private final boolean startStandaloneEngine;
    private final GraknEngineConfig config;

    private EngineContext(boolean startSingleQueueEngine, boolean startStandaloneEngine){
        this.startSingleQueueEngine = startSingleQueueEngine;
        this.startStandaloneEngine = startStandaloneEngine;
        Class<? extends TaskManager> taskManagerClass = startSingleQueueEngine ? RedisTaskManager.class : StandaloneTaskManager.class;
        config = EngineTestUtil.createTestConfig();
        config.setConfigProperty(TASK_MANAGER_IMPLEMENTATION, taskManagerClass.getName());
        server = GraknEngineServer.create(config);
    }

    public static EngineContext singleQueueServer(){
        return new EngineContext( true, false);
    }

    public static EngineContext inMemoryServer(){
        return new EngineContext( true, true);
    }

    public int port() {
        return config.getPropertyAsInt(GraknEngineConfig.SERVER_PORT_NUMBER);
    }

    public GraknEngineServer server() {
        return server;
    }

    public GraknEngineConfig config() {
        return config;
    }

    public TaskManager getTaskManager(){
        return server.getTaskManager();
    }

    public String uri() {
        return config.uri();
    }

    //TODO Rename this method to "sessionWithNewKeyspace"
    public GraknSession factoryWithNewKeyspace() {
        return Grakn.session(uri(), randomKeyspace());
    }

    @Override
    public void before() throws Throwable {
        RestAssured.baseURI = "http://" + uri();
        if (!config.getPropertyAsBool("test.start.embedded.components", true)) {
            return;
        }

        try {
            SimpleURI redisURI = new SimpleURI(config.getProperty(REDIS_HOST));
            EmbeddedRedis.start(redisURI.getPort());
            @Nullable Class<? extends TaskManager> taskManagerClass = null;

            if(startSingleQueueEngine){
                taskManagerClass = RedisTaskManager.class;
            }

            if (startStandaloneEngine){
                taskManagerClass = StandaloneTaskManager.class;
            }

            if (taskManagerClass != null) {
                GraknTestSetup.startCassandraIfNeeded();
                LOG.info("Starting engine on {}", uri());
                server.start();
                LOG.info("Engine started.");
            }
        } catch (Exception e) {
            EmbeddedRedis.stop();
            throw e;
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
                noThrow(() -> {
                    LOG.info("Stopping engine...");
                    // Clear graphs before closing the server because deleting keyspaces needs access to the rest endpoint
                    clearGraphs(server);
                    server.close();
                    LOG.info("Engine stopped.");
                }, "Error closing engine");
            }
            EmbeddedRedis.stop();
        } catch (Exception e){
            throw new RuntimeException("Could not shut down ", e);
        }
    }

    private static void clearGraphs(GraknEngineServer server) {
        // Drop all keyspaces
        final Set<String> keyspaceNames = new HashSet<String>();
        try(GraknTx systemGraph = server.factory().tx(SystemKeyspace.SYSTEM_KB_KEYSPACE, GraknTxType.WRITE)) {
            systemGraph.graql().match(var("x").isa("keyspace-name"))
                    .forEach(x -> x.values().forEach(y -> {
                        keyspaceNames.add(y.asAttribute().getValue().toString());
                    }));
        }

        keyspaceNames.forEach(name -> {
            GraknTx graph = server.factory().tx(name, GraknTxType.WRITE);
            graph.admin().delete();
        });
        server.factory().refreshConnections();
    }
}
