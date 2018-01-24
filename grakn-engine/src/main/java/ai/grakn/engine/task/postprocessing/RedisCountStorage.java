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

package ai.grakn.engine.task.postprocessing;

import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;
import com.codahale.metrics.MetricRegistry;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

/**
 *
 * <p>
 *    Stores a cache of counts so that we can know which {@link ai.grakn.concept.Type}s to shard when they have too many
 *    instances.
 * </p>
 *
 * @author fppt
 */
public class RedisCountStorage {
    private final RedisStorage redisStorage;

    private RedisCountStorage(Pool<Jedis> jedisPool, MetricRegistry metricRegistry){
        redisStorage = new RedisStorage(jedisPool, metricRegistry);
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
        return redisStorage.contactRedis(jedis -> {
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
        return redisStorage.contactRedis(jedis -> {
            String value = jedis.get(key);
            if(value == null) return 0L;
            return Long.parseLong(value);
        });
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
