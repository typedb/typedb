/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.engine;

import ai.grakn.GraknConfigKey;
import ai.grakn.Keyspace;
import ai.grakn.engine.attribute.uniqueness.AttributeUniqueness;
import ai.grakn.engine.data.QueueSanityCheck;
import ai.grakn.engine.data.RedisSanityCheck;
import ai.grakn.engine.controller.HttpController;
import ai.grakn.engine.data.RedisWrapper;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.lock.JedisLockProvider;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.rpc.SessionService;
import ai.grakn.engine.rpc.ServerOpenRequest;
import ai.grakn.engine.task.postprocessing.CountPostProcessor;
import ai.grakn.engine.task.postprocessing.CountStorage;
import ai.grakn.engine.task.postprocessing.IndexPostProcessor;
import ai.grakn.engine.task.postprocessing.IndexStorage;
import ai.grakn.engine.task.postprocessing.PostProcessingTask;
import ai.grakn.engine.task.postprocessing.PostProcessor;
import ai.grakn.engine.task.postprocessing.redisstorage.RedisCountStorage;
import ai.grakn.engine.task.postprocessing.redisstorage.RedisIndexStorage;
import ai.grakn.engine.util.EngineID;
import ai.grakn.engine.rpc.OpenRequest;
import ai.grakn.redismock.RedisServer;
import ai.grakn.test.rule.SessionContext;
import ai.grakn.util.GraknVersion;
import ai.grakn.util.SimpleURI;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Iterables;
import io.grpc.ServerBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemOutRule;
import org.junit.rules.ExpectedException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.util.Pool;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static ai.grakn.util.ErrorMessage.VERSION_MISMATCH;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ServerTest {
    private static final String VERSION_KEY = "info:version";
    private static final String OLD_VERSION = "0.0.1-ontoit-alpha";

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Rule
    public final SystemOutRule stdout = new SystemOutRule();
    @Rule
    public final SessionContext sessionContext = SessionContext.create();

    GraknConfig config = GraknConfig.create();
    spark.Service sparkHttp = spark.Service.ignite();
    RedisWrapper mockRedisWrapper = mock(RedisWrapper.class);
    Jedis mockJedis = mock(Jedis.class);
    private KeyspaceStore keyspaceStore;

    @Before
    public void setUp() {
        Pool<Jedis> jedisPool = mock(JedisPool.class);
        when(mockRedisWrapper.getJedisPool()).thenReturn(jedisPool);
        when(jedisPool.getResource()).thenReturn(mockJedis);
    }

    @After
    public void tearDown() {
        sparkHttp.stop();
    }


    @Test
    public void whenEngineServerIsStarted_SystemKeyspaceIsLoaded() throws IOException {
        SimpleURI uri = new SimpleURI(Iterables.getOnlyElement(config.getProperty(GraknConfigKey.REDIS_HOST)));
        RedisServer redisServer = RedisServer.newRedisServer(uri.getPort());

        redisServer.start();

        try {
            try (Server server = createGraknEngineServer(mockRedisWrapper)) {
                server.start();
                assertNotNull(keyspaceStore);

                // init a random keyspace
                String keyspaceName = "thisisarandomwhalekeyspace";
                keyspaceStore.addKeyspace(Keyspace.of(keyspaceName));

                assertTrue(keyspaceStore.containsKeyspace(Keyspace.of(keyspaceName)));
            }
        } finally {
            redisServer.stop();
        }
    }

    @Test
    public void whenStartingEngineServer_EnsureBackgroundTasksAreRegistered() throws IOException {
        try (Server server = createGraknEngineServer(mockRedisWrapper)) {
            assertThat(server.backgroundTaskRunner().tasks(), hasItem(isA(PostProcessingTask.class)));
        }
    }

    @Test
    public void whenEngineServerIsStartedTheFirstTime_TheVersionIsRecordedInRedis() throws IOException {
        when(mockJedis.get(VERSION_KEY)).thenReturn(null);

        try (Server server = createGraknEngineServer(mockRedisWrapper)) {
            server.start();
        }

        verify(mockJedis).set(VERSION_KEY, GraknVersion.VERSION);
    }

    @Test
    public void whenEngineServerIsStartedASecondTime_TheVersionIsNotChanged() throws IOException {
        when(mockJedis.get(VERSION_KEY)).thenReturn(GraknVersion.VERSION);

        try (Server server = createGraknEngineServer(mockRedisWrapper)) {
            server.start();
        }

        verify(mockJedis, never()).set(eq(VERSION_KEY), any());
    }

    @Test
    @Ignore("Printed but not detected")
    public void whenEngineServerIsStartedWithDifferentVersion_PrintWarning() throws IOException {
        when(mockJedis.get(VERSION_KEY)).thenReturn(OLD_VERSION);
        stdout.enableLog();

        try (Server server = createGraknEngineServer(mockRedisWrapper)) {
            server.start();
        }

        verify(mockJedis).get(VERSION_KEY);
        assertThat(stdout.getLog(), containsString(VERSION_MISMATCH.getMessage(GraknVersion.VERSION, OLD_VERSION)));
    }

    @Test
    public void whenEngineServerIsStartedWithDifferentVersion_TheVersionIsNotChanged() throws IOException {
        when(mockJedis.get(VERSION_KEY)).thenReturn(OLD_VERSION);

        try (Server server = createGraknEngineServer(mockRedisWrapper)) {
            server.start();
        }

        verify(mockJedis, never()).set(eq(VERSION_KEY), any());
    }

    private Server createGraknEngineServer(RedisWrapper redisWrapper) {
        // grakn engine configuration
        EngineID engineId = EngineID.me();
        ServerStatus status = new ServerStatus();

        MetricRegistry metricRegistry = new MetricRegistry();

        // distributed locks
        LockProvider lockProvider = new JedisLockProvider(redisWrapper.getJedisPool());

        keyspaceStore = KeyspaceStoreFake.of();

        // tx-factory
        EngineGraknTxFactory engineGraknTxFactory = EngineGraknTxFactory.create(lockProvider, config, keyspaceStore);


        // post-processing
        IndexStorage indexStorage =  RedisIndexStorage.create(redisWrapper.getJedisPool(), metricRegistry);
        CountStorage countStorage = RedisCountStorage.create(redisWrapper.getJedisPool(), metricRegistry);
        IndexPostProcessor indexPostProcessor = IndexPostProcessor.create(lockProvider, indexStorage);
        CountPostProcessor countPostProcessor = CountPostProcessor.create(config, engineGraknTxFactory, lockProvider, metricRegistry, countStorage);
        PostProcessor postProcessor = PostProcessor.create(indexPostProcessor, countPostProcessor);
        AttributeUniqueness attributeUniqueness = new AttributeUniqueness(config, engineGraknTxFactory);

        // http services: spark, http controller, and gRPC server
        spark.Service sparkHttp = spark.Service.ignite();
        Collection<HttpController> httpControllers = Collections.emptyList();
        int grpcPort = config.getProperty(GraknConfigKey.GRPC_PORT);
        OpenRequest requestOpener = new ServerOpenRequest(engineGraknTxFactory);
        io.grpc.Server server = ServerBuilder.forPort(grpcPort).addService(new SessionService(requestOpener, postProcessor, attributeUniqueness)).build();
        ServerRPC rpcServerRPC = ServerRPC.create(server);
        QueueSanityCheck queueSanityCheck = new RedisSanityCheck(redisWrapper);
        return ServerFactory.createServer(engineId, config, status,
                sparkHttp, httpControllers, rpcServerRPC,
                engineGraknTxFactory, metricRegistry,
                queueSanityCheck, lockProvider, postProcessor, attributeUniqueness, keyspaceStore);
    }
}
