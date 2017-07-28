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

import ai.grakn.engine.util.SimpleURI;
import ai.grakn.util.MockRedisRule;
import com.github.zxl0714.redismock.RedisServer;
import static org.junit.Assert.assertTrue;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class RedisWrapperTest {
    @ClassRule
    public static MockRedisRule mockRedisRule = new MockRedisRule();

    @Test
    public void whenBuildingNoSentinelWellFormed_Succeeds() {
        RedisServer server = mockRedisRule.getServer();
        RedisWrapper redisWrapper = RedisWrapper.builder().setUseSentinel(false)
                .addURI(new SimpleURI(server.getHost(), server.getBindPort()).toString())
                .build();
        assertTrue(redisWrapper.getJedisPool() != null);
    }

    @Test(expected = JedisConnectionException.class)
    public void whenBuildingSentinelWellFormed_JedisCantConnect() {
        RedisServer server = mockRedisRule.getServer();
        RedisWrapper.builder().setUseSentinel(true)
                .addURI(new SimpleURI(server.getHost(), server.getBindPort()).toString())
                .setMasterName("masterName")
                .build();
    }

    @Test(expected = IllegalStateException.class)
    public void whenBuildingSentinelNoMaster_Fails() {
        RedisServer server = mockRedisRule.getServer();
        RedisWrapper.builder().setUseSentinel(true)
                .addURI(new SimpleURI(server.getHost(), server.getBindPort()).toString())
                .build();
    }
}