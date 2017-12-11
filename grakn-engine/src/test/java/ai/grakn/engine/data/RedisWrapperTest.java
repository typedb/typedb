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

package ai.grakn.engine.data;

import ai.grakn.util.SimpleURI;
import ai.grakn.redismock.RedisServer;
import ai.grakn.test.rule.InMemoryRedisContext;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.exceptions.JedisConnectionException;

import static org.junit.Assert.assertNotNull;

public class RedisWrapperTest {
    @ClassRule
    public static InMemoryRedisContext inMemoryRedisContext = InMemoryRedisContext.create();

    @Test
    public void whenBuildingNoSentinelWellFormed_Succeeds() {
        RedisServer server = inMemoryRedisContext.server();
        RedisWrapper redisWrapper = RedisWrapper.builder().setUseSentinel(false)
                .addURI(new SimpleURI(server.getHost(), server.getBindPort()).toString())
                .build();
        assertNotNull(redisWrapper.getJedisPool());
    }

    @Test
    public void whenBuildingFromStringWithPort_Succeeds() {
        RedisWrapper redisWrapper = RedisWrapper.builder().setUseSentinel(false)
                .addURI("localhost:2345")
                .build();
        assertNotNull(redisWrapper.getJedisPool());
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenBuildingFromStringWithDoublePort_Fails() {
        RedisWrapper redisWrapper = RedisWrapper.builder().setUseSentinel(false)
                .addURI("localhost:2345:5678")
                .build();
        assertNotNull(redisWrapper.getJedisPool());
    }

    @Test(expected = JedisConnectionException.class)
    public void whenBuildingSentinelWellFormed_JedisCantConnect() {
        RedisServer server = inMemoryRedisContext.server();
        RedisWrapper.builder().setUseSentinel(true)
                .addURI(new SimpleURI(server.getHost(), server.getBindPort()).toString())
                .setMasterName("masterName")
                .build();
    }

    @Test(expected = IllegalStateException.class)
    public void whenBuildingSentinelNoMaster_Fails() {
        RedisServer server = inMemoryRedisContext.server();
        RedisWrapper.builder().setUseSentinel(true)
                .addURI(new SimpleURI(server.getHost(), server.getBindPort()).toString())
                .build();
    }
}