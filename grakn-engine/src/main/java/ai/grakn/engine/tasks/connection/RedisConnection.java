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

package ai.grakn.engine.tasks.connection;

import ai.grakn.engine.GraknEngineConfig;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * <p>
 *     Connection To Redis Server
 * </p>
 *
 * <p>
 *    A class which manages the connection to the central Redis cache.
 * </p>
 *
 * @author fppt
 */
public class RedisConnection {
    private static final GraknEngineConfig config = GraknEngineConfig.getInstance();
    private static RedisConnection redis;

    private JedisPool jedisPool;

    private RedisConnection(){
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(128);
        jedisPool = new JedisPool(poolConfig, config.getProperty(GraknEngineConfig.REDIS_SERVER_URL),
                config.getPropertyAsInt(GraknEngineConfig.REDIS_SERVER_PORT));
    }

    /**
     * Returns and possibly initliases a connection pool to the redis server.
     *
     * @return a connction to the redis server.
     */
    public static RedisConnection getConnection(){
        if(redis == null) redis = new RedisConnection();
        return redis;
    }
}
