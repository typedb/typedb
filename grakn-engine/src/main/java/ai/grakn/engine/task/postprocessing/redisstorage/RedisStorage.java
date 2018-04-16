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
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.engine.task.postprocessing.redisstorage;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.Pool;

import java.util.function.Function;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * <p>
 *     Redis Server Connection Manager
 * </p>
 *
 * <p>
 *     Provides helper methods for talking with redis.
 *     Used to post process {@link ai.grakn.concept.Concept}s.
 * </p>
 *
 * @author fppt
 */
public class RedisStorage {
    private final static Logger LOG = LoggerFactory.getLogger(RedisStorage.class);
    private final Timer contactRedisTimer;
    private Pool<Jedis> jedisPool;

    public RedisStorage(Pool<Jedis> jedisPool, MetricRegistry metricRegistry){
        this.jedisPool = jedisPool;
        this.contactRedisTimer = metricRegistry.timer(name(RedisCountStorage.class, "contact"));
    }

    /**
     * A helper function which acquires a connection to redis from the pool and then uses it for some operations.
     * This function ensures the connection is closed properly.
     *
     * @param function The function which contactes redis and returns some result
     * @param <X> The type of the result returned.
     * @return The result of contacting redis.
     */
    public <X> X contactRedis(Function<Jedis, X> function){
        try(Jedis jedis = jedisPool.getResource(); Timer.Context ignored = contactRedisTimer.time()){
            return function.apply(jedis);
        } catch (JedisException e) {
            LOG.error("Could not contact redis. Active: {}. Idle: {}", jedisPool.getNumActive(), jedisPool.getNumIdle(), e);
            throw e;
        }
    }
}
