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

package ai.grakn.generator;

import ai.grakn.GraknConfigKey;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.lock.ProcessWideLockProvider;
import ai.grakn.engine.tasks.manager.TaskManager;
import ai.grakn.engine.tasks.manager.redisqueue.RedisTaskManager;
import ai.grakn.engine.util.EngineID;
import ai.grakn.util.SimpleURI;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Iterables;
import com.pholser.junit.quickcheck.generator.GenerationStatus;
import com.pholser.junit.quickcheck.generator.Generator;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.util.Pool;

import java.io.IOException;

/**
 * TaskManagers
 *
 * @author alexandraorth
 */
public class TaskManagers extends Generator<TaskManager> {

    private static final Logger LOG = LoggerFactory.getLogger(TaskManagers.class);

    private static TaskManager taskManager = null;

    public static void close() {
        try {
            taskManager.close();
        } catch (IOException e) {
            LOG.error("Could not close task manager cleanly, some resources might be left open");
        }
        taskManager = null;
    }

    public TaskManagers() {
        super(TaskManager.class);
    }

    @Override
    public TaskManager generate(SourceOfRandomness random, GenerationStatus status) {
        GraknEngineConfig config = GraknEngineConfig.create();
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        SimpleURI simpleURI = new SimpleURI(Iterables.getOnlyElement(config.getProperty(GraknConfigKey.REDIS_HOST)));
        Pool<Jedis> jedisPool = new JedisPool(poolConfig, simpleURI.getHost(), simpleURI.getPort());
        if (taskManager == null) {
            // TODO this doesn't take a Redis connection. Make sure this is what we expect
            taskManager = new RedisTaskManager(EngineID.me(), config, jedisPool, 32, null,
                            new ProcessWideLockProvider(), new MetricRegistry());
        }

        return taskManager;
    }
}
