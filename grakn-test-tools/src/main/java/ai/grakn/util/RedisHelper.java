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

import org.slf4j.LoggerFactory;
import redis.embedded.RedisServer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 *     Starts Embedded Redis
 * </p>
 *
 * <p>
 *     Helper class for starting and working with an embedded redis.
 *     This should be used for testing purposes only
 * </p>
 *
 * @author fppt
 *
 */
public class RedisHelper {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(RedisHelper.class);
    private static AtomicInteger REDIS_COUNTER = new AtomicInteger(0);
    private static RedisServer redisServer;

    /**
     * Starts an embedded redis on the provided port
     *
     * @param port The port to start redis on
     */
    public static void startEmbedded(int port){
        if(REDIS_COUNTER.getAndIncrement() == 0) {
            LOG.info("Starting redis...");
            try {
                redisServer = new RedisServer(port);
                redisServer.start();
            } catch (IOException e){
                throw new RuntimeException("Unable to start embedded redis", e);
            }
            LOG.info("Redis started.");
        }
    }

    /**
     * Stops the embedded redis
     */
    public static void stopEmbedded(){
        if (REDIS_COUNTER.decrementAndGet() == 0) {
            LOG.info("Stopping Redis...");
            redisServer.stop();
            LOG.info("Redis stopped.");
        }
    }
}
