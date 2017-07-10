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

import ai.grakn.concept.ConceptId;
import java.util.function.Function;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

/**
 * <p>
 *     Connection To Redis Server
 * </p>
 *
 * <p>
 *    Given a pool of connections to Redis, it manages the counting
 * </p>
 *
 * @author fppt
 */
public class RedisCountStorage {
    private Pool<Jedis> jedisPool;

    private RedisCountStorage(Pool<Jedis> jedisPool){
        this.jedisPool = jedisPool;
    }

    public static RedisCountStorage create(Pool<Jedis> jedisPool) {
        return new RedisCountStorage(jedisPool);
    }

    /**
     * Adjusts the count for a specific key.
     *
     * @param key the key of the value to adjust
     * @param count the number to adjust the key by
     * @return true
     */
    public long adjustCount(String key, long count){
        return contactRedis(jedis -> {
            if(count != 0) {
                return jedis.incrBy(key, count); //Number is decremented when count is negative
            } else {
               return getCount(key);
            }
        });
    }

    /**
     * Gets the count for the specified key. A count of 0 is returned if the key is not in redis
     *
     * @param key the key stored in redis
     * @return the current count.
     */
    public long getCount(String key){
        return contactRedis(jedis -> {
            String value = jedis.get(key);
            if(value == null) return 0L;
            return Long.parseLong(value);
        });
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

    /**
     * All the valid keys which map to values in the redis cache
     */
    public static String getKeyNumInstances(String keyspace, ConceptId conceptId){
        return "NI_"+ keyspace + "_" + conceptId.getValue();
    }
    public static String getKeyNumShards(String keyspace, ConceptId conceptId){
        return "NS_" + keyspace + "_" + conceptId.getValue();
    }
}
