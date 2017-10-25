/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Ltd
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

package ai.grakn.test.engine;

import ai.grakn.GraknConfigKey;
import ai.grakn.Keyspace;
import ai.grakn.engine.EngineTestHelper;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.GraknEngineServer;
import ai.grakn.engine.data.RedisWrapper;
import ai.grakn.engine.tasks.manager.redisqueue.RedisTaskManager;
import ai.grakn.util.SimpleURI;
import ai.grakn.redismock.RedisServer;
import ai.grakn.test.GraknTestSetup;
import ai.grakn.util.GraknVersion;
import ai.grakn.util.MockRedisRule;
import com.google.common.collect.Iterables;
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

import static ai.grakn.util.ErrorMessage.VERSION_MISMATCH;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GraknEngineServerTest {

    private static final String OLD_VERSION = "0.0.1-ontoit-alpha";

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Rule
    public final SystemOutRule stdout = new SystemOutRule();

    private final GraknEngineConfig conf = GraknEngineConfig.create();
    private static final String VERSION_KEY = "info:version";

    private final RedisWrapper redisWrapper = mock(RedisWrapper.class);
    private final Jedis jedis = mock(Jedis.class);

    @Before
    public void setUp() {
        GraknTestSetup.startCassandraIfNeeded();
        Pool<Jedis> jedisPool = mock(JedisPool.class);
        when(redisWrapper.getJedisPool()).thenReturn(jedisPool);
        when(jedisPool.getResource()).thenReturn(jedis);
    }

    @Test
    public void whenEnginePropertiesIndicatesSingleQueueTM_SingleQueueTmIsStarted() throws IOException {
        MockRedisRule mock = MockRedisRule.create(new SimpleURI(Iterables.getOnlyElement(conf.getProperty(GraknConfigKey.REDIS_HOST))).getPort());
        mock.server().start();

        // Start Engine
        try (GraknEngineServer server = EngineTestHelper.cleanGraknEngineServer(conf)) {
            server.start();
            assertThat(server.getTaskManager(), instanceOf(RedisTaskManager.class));
        }

        mock.server().stop();
    }

    @Test
    public void whenEngineServerIsStarted_SystemKeyspaceIsLoaded() throws IOException {
        GraknTestSetup.startCassandraIfNeeded();
        RedisServer redisServer = MockRedisRule.create(new SimpleURI(Iterables.getOnlyElement(conf.getProperty(GraknConfigKey.REDIS_HOST))).getPort()).server();
        redisServer.start();

        try (GraknEngineServer server = EngineTestHelper.cleanGraknEngineServer(conf)) {
            server.start();
            assertNotNull(server.factory().systemKeyspace());

            // init a random keyspace
            String keyspaceName = "thisisarandomwhalekeyspace";
            server.factory().systemKeyspace().ensureKeyspaceInitialised(Keyspace.of(keyspaceName));

            assertTrue(server.factory().systemKeyspace().containsKeyspace(Keyspace.of(keyspaceName)));
        }

        redisServer.stop();
    }

    @Test
    public void whenEngineServerIsStartedTheFirstTime_TheVersionIsRecordedInRedis() {
        when(jedis.get(VERSION_KEY)).thenReturn(null);

        try (GraknEngineServer server = EngineTestHelper.cleanGraknEngineServer(conf, redisWrapper)) {
            server.start();
        }

        verify(jedis).set(VERSION_KEY, GraknVersion.VERSION);
    }

    @Test
    public void whenEngineServerIsStartedASecondTime_TheVersionIsNotChanged() {
        when(jedis.get(VERSION_KEY)).thenReturn(GraknVersion.VERSION);

        try (GraknEngineServer server = EngineTestHelper.cleanGraknEngineServer(conf, redisWrapper)) {
            server.start();
        }

        verify(jedis, never()).set(eq(VERSION_KEY), any());
    }

    @Test
    @Ignore("Printed but not detected")
    public void whenEngineServerIsStartedWithDifferentVersion_PrintWarning() {
        when(jedis.get(VERSION_KEY)).thenReturn(OLD_VERSION);
        stdout.enableLog();

        try (GraknEngineServer server = EngineTestHelper.cleanGraknEngineServer(conf, redisWrapper)) {
            server.start();
        }

        verify(jedis).get(VERSION_KEY);
        assertThat(stdout.getLog(), containsString(VERSION_MISMATCH.getMessage(GraknVersion.VERSION, OLD_VERSION)));
    }

    @Test
    public void whenEngineServerIsStartedWithDifferentVersion_TheVersionIsNotChanged() {
        when(jedis.get(VERSION_KEY)).thenReturn(OLD_VERSION);

        try (GraknEngineServer server = EngineTestHelper.cleanGraknEngineServer(conf, redisWrapper)) {
            server.start();
        }

        verify(jedis, never()).set(eq(VERSION_KEY), any());
    }
}
