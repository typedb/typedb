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
package ai.grakn.util;

import ai.grakn.redismock.RedisServer;
import org.junit.rules.ExternalResource;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Rule class for executing tests that require a Redis mock
 *
 * @author pluraliseseverythings
 */
public class MockRedisRule extends ExternalResource {
    private static final JedisPoolConfig DEFAULT_CONFIG = new JedisPoolConfig();
    private final Map<JedisPoolConfig, JedisPool> pools = new HashMap<>();
    private RedisServer server;

    private MockRedisRule(int port) {
        try {
            server = RedisServer.newRedisServer(port);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static MockRedisRule create(){
        return create(0);
    }

    public static MockRedisRule create(int port){
        return new MockRedisRule(port);
    }

    @Override
    protected void before() throws Throwable {
        server.start();
    }

    @Override
    protected void after() {
        if (server != null) {
            server.stop();
            server = null;
        }
    }

    public RedisServer server() {
        return server;
    }

    public JedisPool jedisPool(){
        return jedisPool(DEFAULT_CONFIG);
    }

    public JedisPool jedisPool(JedisPoolConfig config){
        if(!pools.containsKey(config)){
            JedisPool pool = new JedisPool(config, server.getHost(), server.getBindPort(), 1000000);
            pools.put(config, pool);
        }
        return pools.get(config);
    }
}
