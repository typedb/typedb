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

import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.LoggerFactory;
import redis.embedded.RedisServer;
import redis.embedded.exceptions.EmbeddedRedisException;

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
public class EmbeddedRedis {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(EmbeddedRedis.class);
    private static AtomicInteger REDIS_COUNTER = new AtomicInteger(0);
    private static volatile RedisServer redisServer;

    static {
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    if (redisServer != null && redisServer.isActive()) {
                        LOG.warn("Redis still running, stopping it on shutdown hook");
                        redisServer.stop();
                    }
                }));
    }

    /**
     * Starts an embedded redis on the provided port
     *
     * @param port The port to start redis on
     */
    public static void start(int port){
        if(REDIS_COUNTER.getAndIncrement() == 0) {
            LOG.info("Starting redis...");
            redisServer = RedisServer.builder()
                    .port(port)
                    // We have short running tests and sometimes we kill the connections
                    .setting("timeout 360")
                    .build();
            if (!redisServer.isActive()) {
                try {
                    redisServer.start();
                } catch (EmbeddedRedisException e) {
                    LOG.warn("Unexpected Redis instance already running on port {}", port, e);
                } catch (Exception e) {
                    LOG.warn("Exception while trying to start Redis on port {}. Will attempt to continue.", port, e);
                }
                LOG.info("Redis started on {}", port);
            } else {
                LOG.warn("Redis already running.");
            }
        }
    }

    /**
     * Stops the embedded redis
     */
    public static void stop(){
        if (redisServer != null && REDIS_COUNTER.decrementAndGet() <= 0) {
            LOG.info("Stopping Redis...");
            redisServer.stop();
            LOG.info("Redis stopped.");
        } else {
            LOG.warn("Called stop while {} redis instances are running", REDIS_COUNTER);
        }
    }
}
