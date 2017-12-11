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

package ai.grakn.engine.postprocessing;

import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.Pool;

import java.util.function.Function;

import static com.codahale.metrics.MetricRegistry.name;

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
    private final static Logger LOG = LoggerFactory.getLogger(RedisCountStorage.class);

    private final Timer contactRedisTimer;
    private Pool<Jedis> jedisPool;

    private RedisCountStorage(Pool<Jedis> jedisPool, MetricRegistry metricRegistry){
        this.jedisPool = jedisPool;
        this.contactRedisTimer = metricRegistry.timer(name(RedisCountStorage.class, "contact"));
    }

    public static RedisCountStorage create(Pool<Jedis> jedisPool, MetricRegistry metricRegistry) {
        return new RedisCountStorage(jedisPool, metricRegistry);
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
        try(Jedis jedis = jedisPool.getResource(); Context ignored = contactRedisTimer.time()){
            return function.apply(jedis);
        } catch (JedisException e) {
            LOG.error("Could not contact redis. Active: {}. Idle: {}", jedisPool.getNumActive(), jedisPool.getNumIdle(), e);
            throw e;
        }
    }

    /**
     * All the valid keys which map to values in the redis cache
     */
    public static String getKeyNumInstances(Keyspace keyspace, ConceptId conceptId){
        return "NI_"+ keyspace + "_" + conceptId.getValue();
    }
    public static String getKeyNumShards(Keyspace keyspace, ConceptId conceptId){
        return "NS_" + keyspace + "_" + conceptId.getValue();
    }
}
