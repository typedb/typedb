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

package ai.grakn.test.rule;

import org.junit.rules.ExternalResource;
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
public class EmbeddedRedisContext extends ExternalResource {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(EmbeddedRedisContext.class);
    private final int port;
    private RedisServer redisServer;

    private EmbeddedRedisContext(int port) {
        this.port = port;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (redisServer != null && redisServer.isActive()) {
                LOG.warn("Redis still running, stopping it on shutdown hook");
                redisServer.stop();
            }
        }, "shutdown-redis"));
    }

    public static EmbeddedRedisContext create(int port) {
        return new EmbeddedRedisContext(port);
    }

    @Override
    protected void before() throws Throwable {
        try {
            LOG.info("Starting redis...");
            redisServer = RedisServer.builder()
                    .port(port)
                    .setting("timeout 360").build();

            if (!redisServer.isActive()) {
                try {
                    redisServer.start();
                } catch (EmbeddedRedisException e) {
                    LOG.warn("Unexpected Redis instance already running on port {}", port);
                } catch (Exception e) {
                    LOG.warn("Exception while trying to start Redis on port {}. Will attempt to continue.", port, e);
                }
                LOG.info("Redis started on {}", port);
            } else {
                LOG.warn("Redis already running.");
            }
        } catch (Exception e) {
            LOG.warn("Failure to start redis on port {}, maybe running alredy", port, e);
        }
    }

    @Override
    protected void after() {
        try {
            LOG.info("Stopping Redis...");
            redisServer.stop();
            LOG.info("Redis stopped.");
        } catch (Exception e) {
            LOG.warn("Failure while stopping redis", e);
        }
    }
}
