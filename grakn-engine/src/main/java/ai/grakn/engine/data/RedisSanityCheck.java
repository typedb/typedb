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

package ai.grakn.engine.data;

import ai.grakn.util.GraknVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import static ai.grakn.util.ErrorMessage.VERSION_MISMATCH;

/**
 * The {@link RedisSanityCheck} class is responsible for performing sanity check to the Grakn Queue component which is backed by Redis
 *
 * @author Ganeshwara Herawan Hananda
 */
public class RedisSanityCheck implements QueueSanityCheck {
    private static final Logger LOG = LoggerFactory.getLogger(RedisSanityCheck.class);
    private static final String REDIS_VERSION_KEY = "info:version";
    private RedisWrapper redisWrapper;

    public RedisSanityCheck(RedisWrapper redisWrapper) {
        this.redisWrapper = redisWrapper;
    }

    @Override
    public void testConnection() {
        redisWrapper.testConnection();
    }

    @Override
    public void checkVersion() {
        Jedis jedis = redisWrapper.getJedisPool().getResource();
        String storedVersion = jedis.get(REDIS_VERSION_KEY);
        if (storedVersion == null) {
            jedis.set(REDIS_VERSION_KEY, GraknVersion.VERSION);
        } else if (!storedVersion.equals(GraknVersion.VERSION)) {
            LOG.warn(VERSION_MISMATCH.getMessage(GraknVersion.VERSION, storedVersion));
        }
    }

    @Override
    public void close() {
        redisWrapper.close();
    }
}
