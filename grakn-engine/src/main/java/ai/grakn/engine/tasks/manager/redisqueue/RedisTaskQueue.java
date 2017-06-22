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
package ai.grakn.engine.tasks.manager.redisqueue;


import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.util.EngineID;
import com.codahale.metrics.MetricRegistry;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.ConfigBuilder;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.client.Client;
import net.greghaines.jesque.client.ClientPoolImpl;
import static net.greghaines.jesque.utils.JesqueUtils.entry;
import static net.greghaines.jesque.utils.JesqueUtils.map;
import net.greghaines.jesque.worker.MapBasedJobFactory;
import net.greghaines.jesque.worker.RecoveryStrategy;
import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerEvent;
import net.greghaines.jesque.worker.WorkerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;


/**
 * Queue implemented in redis
 *
 * @author Domenico Corapi
 */
class RedisTaskQueue {
    private final static Logger LOG = LoggerFactory.getLogger(RedisTaskQueue.class);

    private static final String QUEUE_NAME = "grakn_engine_queue";

    private final Client redisClient;
    private final Config config;
    private final MetricRegistry metricRegistry;

    RedisTaskQueue(Pool<Jedis> jedisPool, MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
        this.config = new ConfigBuilder().build();
        this.redisClient = new ClientPoolImpl(config, jedisPool);
    }

    void close() throws IOException {
        redisClient.end();
    }

    void putJob(Task job) {
        final Job queueJob = new Job(Task.class.getName(), job);
        redisClient.enqueue(QUEUE_NAME, queueJob);
    }

    void subscribe(
            RedisTaskManager redisTaskManager,
            ExecutorService consumerExecutor,
            EngineID engineId, GraknEngineConfig config,
            Pool<Jedis> jedisPool) {
        Worker worker = getJobSubscriber();

        // We need this since the job can only be instantiated with the
        // task coming from the queue
        worker.getWorkerEventEmitter().addListener(
                (event, worker1, queue, job, runner, result, t) -> {
                    if (runner instanceof RedisTaskQueueConsumer) {
                        ((RedisTaskQueueConsumer) runner).setRunningState(redisTaskManager, engineId, config, jedisPool, metricRegistry);
                    }
                }, WorkerEvent.JOB_EXECUTE);

        worker.setExceptionHandler((jobExecutor, exception, curQueue) -> {
            // TODO review this strategy
            LOG.error("Exception while trying to run task", exception);
            return RecoveryStrategy.TERMINATE;
        });

        consumerExecutor.execute(worker);
    }

    private Worker getJobSubscriber() {
        return new WorkerImpl(config, Collections.singletonList(QUEUE_NAME),
                new MapBasedJobFactory(
                        map(entry(
                                // Assign elements with this class
                                Task.class.getName(),
                                // To be run by this class
                                RedisTaskQueueConsumer.class))));
    }
}