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

package ai.grakn.test.rule;

import ai.grakn.Grakn;
import ai.grakn.GraknConfigKey;
import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.engine.GraknCreator;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.GraknEngineServer;
import ai.grakn.engine.SystemKeyspace;
import ai.grakn.engine.postprocessing.RedisCountStorage;
import ai.grakn.engine.tasks.manager.TaskManager;
import ai.grakn.engine.tasks.mock.MockBackgroundTask;
import ai.grakn.util.GraknTestUtil;
import ai.grakn.util.SimpleURI;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.jayway.restassured.RestAssured;
import org.junit.rules.TestRule;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static ai.grakn.engine.util.ExceptionWrapper.noThrow;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.SampleKBLoader.randomKeyspace;


/**
 * <p>
 * Start the Grakn Engine server before each test class and stop after.
 * </p>
 *
 * @author alexandraorth
 */
public class EngineContext extends CompositeTestRule {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(EngineContext.class);

    private GraknEngineServer server;

    private final GraknEngineConfig config = createTestConfig();
    private JedisPool jedisPool;

    private final TestRule redis;

    private EngineContext(boolean inMemoryRedis){
        SimpleURI redisURI = new SimpleURI(Iterables.getOnlyElement(config.getProperty(GraknConfigKey.REDIS_HOST)));
        int redisPort = redisURI.getPort();

        if (inMemoryRedis) {
            redis = InMemoryRedisContext.create(redisPort);
        } else {
            redis = EmbeddedRedisContext.create(redisPort);
        }
    }

    /**
     * Creates a {@link EngineContext} for testing which uses a real embedded redis.
     * This should only be used for benchmark testing where performance and memory usage matters.
     *
     * @return a new {@link EngineContext} for testing
     */
    public static EngineContext createWithEmbeddedRedis(){
        return new EngineContext(false);
    }

    /**
     * Creates a {@link EngineContext} for testing which uses an in-memory redis mock.
     * This is the default test environment which should be used because starting an embedded redis is a costly process.
     *
     * @return a new {@link EngineContext} for testing
     */
    public static EngineContext createWithInMemoryRedis(){
        return new EngineContext(true);
    }

    public GraknEngineServer server() {
        return server;
    }

    public GraknEngineConfig config() {
        return config;
    }

    public RedisCountStorage redis() {
        return redis(Iterables.getOnlyElement(config.getProperty(GraknConfigKey.REDIS_HOST)));
    }

    public RedisCountStorage redis(String uri) {
        SimpleURI simpleURI = new SimpleURI(uri);
        return redis(simpleURI.getHost(), simpleURI.getPort());
    }

    public RedisCountStorage redis(String host, int port) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        this.jedisPool = new JedisPool(poolConfig, host, port);
        MetricRegistry metricRegistry = new MetricRegistry();
        return RedisCountStorage.create(jedisPool, metricRegistry);
    }

    public TaskManager getTaskManager(){
        return server.getTaskManager();
    }

    public SimpleURI uri() {
        return config.uri();
    }

    public GraknSession sessionWithNewKeyspace() {
        return Grakn.session(uri(), randomKeyspace());
    }

    @Override
    protected List<TestRule> testRules() {
        return ImmutableList.of(
                SessionContext.create(),
                redis
        );
    }

    @Override
    public void before() throws Throwable {
        RestAssured.baseURI = uri().toURI().toString();
        if (!config.getProperty(GraknConfigKey.TEST_START_EMBEDDED_COMPONENTS)) {
            return;
        }

        SimpleURI redisURI = new SimpleURI(Iterables.getOnlyElement(config.getProperty(GraknConfigKey.REDIS_HOST)));

        jedisPool = new JedisPool(redisURI.getHost(), redisURI.getPort());

        // To ensure consistency b/w test profiles and configuration files, when not using Janus
        // for a unit tests in an IDE, add the following option:
        // -Dgrakn.conf=../conf/test/tinker/grakn.properties
        //
        // When using janus, add -Dgrakn.test-profile=janus
        //
        // The reason is that the default configuration of Grakn uses the Janus Factory while the default
        // test profile is tinker: so when running a unit test within an IDE without any extra parameters,
        // we end up wanting to use the JanusFactory but without starting Cassandra first.
        LOG.info("starting engine...");

        // start engine
        setRestAssuredUri(config);
        server = GraknCreator.cleanGraknEngineServer(config);
        server.start();

        LOG.info("engine started.");
    }

    @Override
    public void after() {
        if (!config.getProperty(GraknConfigKey.TEST_START_EMBEDDED_COMPONENTS)) {
            return;
        }
        noThrow(MockBackgroundTask::clearTasks, "Error clearing tasks");

        try {
            noThrow(() -> {
                LOG.info("stopping engine...");

                // Clear graphs before closing the server because deleting keyspaces needs access to the rest endpoint
                clearGraphs(server);
                server.close();

                LOG.info("engine stopped.");

                // There is no way to stop the embedded Casssandra, no such API offered.
            }, "Error closing engine");
            jedisPool.close();
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

    /**
     * Create a configuration for use in tests, using random ports.
     */
    private static GraknEngineConfig createTestConfig() {
        GraknEngineConfig config = GraknEngineConfig.create();

        config.setConfigProperty(GraknConfigKey.SERVER_PORT, GraknTestUtil.getEphemeralPort());
        config.setConfigProperty(GraknConfigKey.REDIS_HOST, Collections.singletonList("localhost:" + GraknTestUtil.getEphemeralPort()));

        return config;
    }

    private static void setRestAssuredUri(GraknEngineConfig config) {
        RestAssured.baseURI = "http://" + config.uri();
    }

    public JedisPool getJedisPool() {
        return jedisPool;
    }
}
