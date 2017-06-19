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
 *
 */
package ai.grakn.engine.tasks.manager;


import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.ConfigBuilder;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.client.Client;
import net.greghaines.jesque.client.ClientPoolImpl;
import static net.greghaines.jesque.utils.JesqueUtils.entry;
import static net.greghaines.jesque.utils.JesqueUtils.map;
import net.greghaines.jesque.worker.MapBasedJobFactory;
import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerImpl;
import redis.clients.jedis.JedisPool;


/**
 * Queue implemented in redis
 * @param <T>   Type of object in the queue
 *
 * @author Domenico Corapi
 */
public class RedisTaskQueue<T extends QueableTask> implements Closeable {

    private static final String QUEUE_NAME = "grakn_engine_queue";

    private final Client redisClient;
    private final Config config;


    public RedisTaskQueue(JedisPool jedisPool) {
        this.config = new ConfigBuilder().build();
        this.redisClient = new ClientPoolImpl(config, jedisPool);
    }

    public void putJob(T job) {
        final Job queueJob = new Job(job.getClass().getName(), job);
        redisClient.enqueue(QUEUE_NAME, queueJob);
    }


    @Override
    public void close() throws IOException {
        redisClient.end();
    }

    public Worker getJobSubscriber() {
        return new WorkerImpl(config, Collections.singletonList(QUEUE_NAME),
                new MapBasedJobFactory(map(entry(RedisTaskQueueConsumer.JobRunner.class.getName(), RedisTaskQueueConsumer.JobRunner.class))));
    }


}