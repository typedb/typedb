/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.test.rule;

import ai.grakn.GraknConfigKey;
import ai.grakn.GraknSystemProperty;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.engine.GraknConfig;
import ai.grakn.engine.KeyspaceStore;
import ai.grakn.engine.Server;
import ai.grakn.engine.ServerFactory;
import ai.grakn.engine.ServerRPC;
import ai.grakn.engine.ServerStatus;
import ai.grakn.engine.data.QueueSanityCheck;
import ai.grakn.engine.data.RedisSanityCheck;
import ai.grakn.engine.data.RedisWrapper;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.lock.JedisLockProvider;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.rpc.KeyspaceService;
import ai.grakn.engine.rpc.OpenRequest;
import ai.grakn.engine.rpc.ServerOpenRequest;
import ai.grakn.engine.rpc.SessionService;
import ai.grakn.engine.task.postprocessing.CountPostProcessor;
import ai.grakn.engine.task.postprocessing.CountStorage;
import ai.grakn.engine.task.postprocessing.IndexPostProcessor;
import ai.grakn.engine.task.postprocessing.IndexStorage;
import ai.grakn.engine.task.postprocessing.PostProcessor;
import ai.grakn.engine.task.postprocessing.redisstorage.RedisCountStorage;
import ai.grakn.engine.task.postprocessing.redisstorage.RedisIndexStorage;
import ai.grakn.engine.util.EngineID;
import ai.grakn.factory.EmbeddedGraknSession;
import ai.grakn.keyspace.KeyspaceStoreImpl;
import ai.grakn.util.GraknTestUtil;
import ai.grakn.util.SimpleURI;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.jayway.restassured.RestAssured;
import io.grpc.ServerBuilder;
import org.junit.rules.TestRule;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.SampleKBLoader.randomKeyspace;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;


/**
 * <p>
 * Start the Grakn Engine server before each test class and stop after.
 *
 * NOTE: This Context only works with Janus profile and should only be used for Integration Tests.
 * </p>
 *
 * @author alexandraorth
 */
public class EngineContext extends CompositeTestRule {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(EngineContext.class);

    private Server server;

    private final GraknConfig config;
    private JedisPool jedisPool;
    private spark.Service sparkHttp;

    private final InMemoryRedisContext redis;

    public KeyspaceStore systemKeyspace() {
        return keyspaceStore;
    }

    private KeyspaceStore keyspaceStore;

    public EngineGraknTxFactory factory() {
        return engineGraknTxFactory;
    }

    private EngineGraknTxFactory engineGraknTxFactory;

    private EngineContext() {
        config = createTestConfig();
        redis = InMemoryRedisContext.create();
    }

    private EngineContext(GraknConfig config, InMemoryRedisContext redis) {
        this.config = config;
        this.redis = redis;
    }

    public static EngineContext create(GraknConfig config) {
        SimpleURI redisURI = new SimpleURI(Iterables.getOnlyElement(config.getProperty(GraknConfigKey.REDIS_HOST)));
        int redisPort = redisURI.getPort();
        InMemoryRedisContext redis = InMemoryRedisContext.create(redisPort);
        return new EngineContext(config, redis);
    }

    /**
     * Creates a {@link EngineContext} for testing which uses an in-memory redis mock.
     *
     * @return a new {@link EngineContext} for testing
     */
    public static EngineContext create() {
        return new EngineContext();
    }

    public Server server() {
        return server;
    }

    public GraknConfig config() {
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

    public SimpleURI uri() {
        return config.uri();
    }

    public SimpleURI grpcUri() {
        return new SimpleURI(config.uri().getHost(), config.getProperty(GraknConfigKey.GRPC_PORT));
    }

    public EmbeddedGraknSession sessionWithNewKeyspace() {
        return EmbeddedGraknSession.createEngineSession(randomKeyspace(), config);
    }

    @Override
    protected final List<TestRule> testRules() {
        return ImmutableList.of(
                SessionContext.create(),
                redis
        );
    }

    @Override
    protected final void before() throws Throwable {
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

        sparkHttp = spark.Service.ignite();

        config.setConfigProperty(GraknConfigKey.REDIS_HOST, Collections.singletonList("localhost:" + redis.port()));
        RedisWrapper redis = RedisWrapper.create(config);

        server = startGraknEngineServer(redis);

        LOG.info("engine started on " + uri());
    }

    @Override
    protected final void after() {
        if (!config.getProperty(GraknConfigKey.TEST_START_EMBEDDED_COMPONENTS)) {
            return;
        }

        try {
            noThrow(() -> {
                LOG.info("stopping engine...");

                // Clear graphs before closing the server because deleting keyspaces needs access to the rest endpoint
                clearGraphs();
                server.close();

                LOG.info("engine stopped.");

                // There is no way to stop the embedded Casssandra, no such API offered.
            }, "Error closing engine");
            jedisPool.close();
            sparkHttp.stop();
        } catch (Exception e) {
            throw new RuntimeException("Could not shut down ", e);
        }
    }

    private void clearGraphs() {
        // Drop all keyspaces
        final Set<String> keyspaceNames = new HashSet<String>();
        try (GraknTx systemGraph = engineGraknTxFactory.tx(KeyspaceStore.SYSTEM_KB_KEYSPACE, GraknTxType.WRITE)) {
            systemGraph.graql().match(var("x").isa("keyspace-name"))
                    .forEach(x -> x.concepts().forEach(y -> {
                        keyspaceNames.add(y.asAttribute().value().toString());
                    }));
        }

        keyspaceNames.forEach(name -> keyspaceStore.deleteKeyspace(Keyspace.of(name)));
        engineGraknTxFactory.refreshConnections();
    }

    private static void noThrow(RunnableWithExceptions fn, String errorMessage) {
        try {
            fn.run();
        } catch (Throwable t) {
            LOG.error(errorMessage + "\nThe exception was: " + getFullStackTrace(t));
        }
    }

    /**
     * Function interface that throws exception for use in the noThrow function
     *
     * @param <E>
     */
    @FunctionalInterface
    private interface RunnableWithExceptions<E extends Exception> {
        void run() throws E;
    }

    /**
     * Create a configuration for use in tests, using random ports.
     */
    public static GraknConfig createTestConfig() {
        GraknConfig config = GraknConfig.create();

        config.setConfigProperty(GraknConfigKey.SERVER_PORT, 0);

        return config;
    }

    private static void setRestAssuredUri(GraknConfig config) {
        RestAssured.baseURI = "http://" + config.uri();
    }

    public JedisPool getJedisPool() {
        return jedisPool;
    }

    private Server startGraknEngineServer(RedisWrapper redisWrapper) throws IOException {
        EngineID id = EngineID.me();
        ServerStatus status = new ServerStatus();

        MetricRegistry metricRegistry = new MetricRegistry();

        // distributed locks
        LockProvider lockProvider = new JedisLockProvider(redisWrapper.getJedisPool());

        keyspaceStore = new KeyspaceStoreImpl(config);

        // tx-factory
        engineGraknTxFactory = EngineGraknTxFactory.create(lockProvider, config, keyspaceStore);


        // post-processing
        IndexStorage indexStorage = RedisIndexStorage.create(redisWrapper.getJedisPool(), metricRegistry);
        CountStorage countStorage = RedisCountStorage.create(redisWrapper.getJedisPool(), metricRegistry);
        IndexPostProcessor indexPostProcessor = IndexPostProcessor.create(lockProvider, indexStorage);
        CountPostProcessor countPostProcessor = CountPostProcessor.create(config, engineGraknTxFactory, lockProvider, metricRegistry, countStorage);
        PostProcessor postProcessor = PostProcessor.create(indexPostProcessor, countPostProcessor);
        OpenRequest requestOpener = new ServerOpenRequest(engineGraknTxFactory);

        io.grpc.Server server = ServerBuilder.forPort(0)
                .addService(new SessionService(requestOpener, postProcessor))
                .addService(new KeyspaceService(keyspaceStore))
                .build();
        ServerRPC rpcServerRPC = ServerRPC.create(server);
        GraknTestUtil.allocateSparkPort(config);
        QueueSanityCheck queueSanityCheck = new RedisSanityCheck(redisWrapper);

        Server graknEngineServer = ServerFactory.createServer(id, config, status,
                sparkHttp, Collections.emptyList(), rpcServerRPC,
                engineGraknTxFactory, metricRegistry,
                queueSanityCheck, lockProvider, postProcessor, keyspaceStore);

        graknEngineServer.start();

        // Read the automatically allocated ports and write them back into the config
        config.setConfigProperty(GraknConfigKey.GRPC_PORT, server.getPort());

        return graknEngineServer;
    }

}
