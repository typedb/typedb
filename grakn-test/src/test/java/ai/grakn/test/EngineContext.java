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
import static ai.grakn.engine.GraknEngineConfig.REDIS_HOST;
import static ai.grakn.engine.GraknEngineConfig.TASK_MANAGER_IMPLEMENTATION;
import ai.grakn.engine.GraknEngineServer;
import ai.grakn.engine.tasks.connection.RedisCountStorage;
import ai.grakn.engine.tasks.manager.StandaloneTaskManager;
import ai.grakn.engine.tasks.manager.TaskManager;
import ai.grakn.engine.tasks.manager.redisqueue.RedisTaskManager;
import ai.grakn.engine.tasks.mock.MockBackgroundTask;
import static ai.grakn.engine.util.ExceptionWrapper.noThrow;
import ai.grakn.engine.util.SimpleURI;
import static ai.grakn.test.GraknTestEngineSetup.startEngine;
import static ai.grakn.test.GraknTestEngineSetup.startRedis;
import static ai.grakn.test.GraknTestEngineSetup.stopEngine;
import static ai.grakn.test.GraknTestEngineSetup.stopRedis;
import static ai.grakn.util.GraphLoader.randomKeyspace;
import com.jayway.restassured.RestAssured;
import org.junit.rules.ExternalResource;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import javax.annotation.Nullable;


/**
 * <p>
 * Start the Grakn Engine server before each test class and stop after.
 * </p>
 *
 * @author alexandraorth
 */
public class EngineContext extends ExternalResource {

    private GraknEngineServer server;

    private final boolean startSingleQueueEngine;
    private final boolean startStandaloneEngine;
    private final GraknEngineConfig config = GraknTestEngineSetup.createTestConfig();
    private JedisPool jedisPool;

    private EngineContext(boolean startSingleQueueEngine, boolean startStandaloneEngine){
        this.startSingleQueueEngine = startSingleQueueEngine;
        this.startStandaloneEngine = startStandaloneEngine;
    }

    public static EngineContext startNoQueue(){
        return new EngineContext( false, false);
    }

    public static EngineContext startSingleQueueServer(){
        return new EngineContext( true, false);
    }

    public static EngineContext startInMemoryServer(){
        return new EngineContext( false, true);
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

    public RedisCountStorage redis() {
        return redis(config.getProperty(REDIS_HOST));
    }

    public RedisCountStorage redis(String uri) {
        SimpleURI simpleURI = new SimpleURI(uri);
        return redis(simpleURI.getHost(), simpleURI.getPort());
    }

    public RedisCountStorage redis(String host, int port) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        this.jedisPool = new JedisPool(poolConfig, host, port);
        return RedisCountStorage.create(jedisPool);
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
        RestAssured.baseURI = "http://" + config.getProperty("server.host") + ":" + config.getProperty("server.port");        
        if (!config.getPropertyAsBool("test.start.embedded.components", true)) {
            return;
        }

        try {
            SimpleURI redisURI = new SimpleURI(config.getProperty(REDIS_HOST));
            startRedis(redisURI.getPort());
            jedisPool = new JedisPool(redisURI.getHost(), redisURI.getPort());

            @Nullable Class<? extends TaskManager> taskManagerClass = null;

            if(startSingleQueueEngine){
                taskManagerClass = RedisTaskManager.class;
            }

            if (startStandaloneEngine){
                taskManagerClass = StandaloneTaskManager.class;
            }

            if (taskManagerClass != null) {
                config.setConfigProperty(TASK_MANAGER_IMPLEMENTATION, taskManagerClass.getName());
                server = startEngine(config);
            }
        } catch (Exception e) {
            stopRedis();
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
                noThrow(() -> stopEngine(server), "Error closing engine");
            }
            getJedisPool().close();
            stopRedis();
        } catch (Exception e){
            throw new RuntimeException("Could not shut down ", e);
        }
    }

    public JedisPool getJedisPool() {
        return jedisPool;
    }
}
