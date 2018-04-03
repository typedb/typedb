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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */
package ai.grakn.test.rule;

import ai.grakn.redismock.RedisServer;
import com.google.common.base.Preconditions;
import org.junit.rules.ExternalResource;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Rule class for executing tests that require a Redis mock
 *
 * @author pluraliseseverythings
 */
public class InMemoryRedisContext extends ExternalResource {
    private static final JedisPoolConfig DEFAULT_CONFIG = new JedisPoolConfig();
    private final Map<JedisPoolConfig, JedisPool> pools = new HashMap<>();
    private final int port;
    private @Nullable RedisServer server = null;

    private InMemoryRedisContext(int port) {
        this.port = port;
    }

    public static InMemoryRedisContext create(){
        return create(0);
    }

    public static InMemoryRedisContext create(int port){
        return new InMemoryRedisContext(port);
    }

    @Override
    protected void before() throws Throwable {
        try {
            server = RedisServer.newRedisServer(port);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

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
        checkInitialised();
        assert server != null;
        return server;
    }

    public JedisPool jedisPool(){
        return jedisPool(DEFAULT_CONFIG);
    }

    public JedisPool jedisPool(JedisPoolConfig config){
        checkInitialised();
        assert server != null;

        if(!pools.containsKey(config)){
            JedisPool pool = new JedisPool(config, server.getHost(), server.getBindPort(), 1000000);
            pools.put(config, pool);
        }
        return pools.get(config);
    }

    private void checkInitialised() {
        Preconditions.checkState(server != null, "InMemoryRedisContext not initialised");
    }

    public int port() {
        checkInitialised();
        assert server != null;
        return server.getBindPort();
    }
}
