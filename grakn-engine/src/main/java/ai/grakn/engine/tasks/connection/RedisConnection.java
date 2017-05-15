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

import ai.grakn.concept.TypeLabel;
import ai.grakn.engine.GraknEngineConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.function.Function;

/**
 * <p>
 *     Connection To Redis Server
 * </p>
 *
 * <p>
 *    A class which manages the connection to the central Redis cache.
 *    This serves as a cache for keeping track of the counts of concepts which may be in need of sharding.
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

    //TODO: Make generic when instances require sharding
    /**
     * Increments the number of instances currently on a type.
     *
     * @param keyspace the keyspace the type is in
     * @param label the label of the type
     * @param count the number of instances which have been added/removes
     * @return true
     */
    public long adjustCount(String keyspace, TypeLabel label, long count){
        return contactRedis((jedis) -> {
            String key = getRedisKey(keyspace, label);
            if(count > 0) {
                return jedis.incrBy(key, count);
            } else if (count < 0){
                return jedis.decrBy(key, -1L * count); //If you decrement by a negative number it adds!
            } else {
                String value = jedis.get(key);
                if(value == null) return 0L;
                return Long.parseLong(value);
            }
        });
    }
    private String getRedisKey(String keyspace, TypeLabel label){
        return keyspace + "_" + label.getValue();
    }

    /**
     * A helper function which acquires a connection to redis from the pool and then uses it for some operations.
     * This function ensures the connection is closed properly.
     *
     * @param function The function which contactes redis and returns some result
     * @param <X> The type of the result returned.
     * @return The result of contacting redis.
     */
    private <X> X contactRedis(Function<Jedis, X> function){
        try(Jedis jedis = jedisPool.getResource()){
            return function.apply(jedis);
        }
    }
}
