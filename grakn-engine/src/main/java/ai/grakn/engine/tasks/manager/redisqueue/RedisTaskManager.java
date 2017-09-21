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
import ai.grakn.engine.TaskId;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.tasks.connection.RedisCountStorage;
import ai.grakn.engine.tasks.manager.TaskConfiguration;
import ai.grakn.engine.tasks.manager.TaskManager;
import ai.grakn.engine.tasks.manager.TaskState;
import ai.grakn.engine.util.EngineID;
import ai.grakn.redisq.Redisq;
import ai.grakn.redisq.RedisqBuilder;
import static ai.grakn.redisq.State.DONE;
import static ai.grakn.redisq.State.FAILED;
import ai.grakn.redisq.exceptions.StateFutureInitializationException;
import ai.grakn.redisq.exceptions.WaitException;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

/**
 * Handle the lifecycle of tasks in Redis. Given a jedis pool
 * it starts a set of consumers that subscribe to the task queue.
 * Tasks can be added using the addTask method.
 *
 * @author pluraliseseverythings
 */
public class RedisTaskManager implements TaskManager {

    private static final Logger LOG = LoggerFactory.getLogger(RedisTaskManager.class);
    private static final int TIMEOUT_SECONDS = 5;
    private static final String QUEUE_NAME = "grakn";

    private final Redisq<Task> redisq;
    private final RedisTaskStorage taskStorage;

    public RedisTaskManager(EngineID engineId, GraknEngineConfig config, Pool<Jedis> jedisPool,
            EngineGraknTxFactory factory, LockProvider distributedLockClient,
            MetricRegistry metricRegistry) {
        this(engineId, config, jedisPool, 32, factory, distributedLockClient, metricRegistry);
    }

    public RedisTaskManager(EngineID engineId, GraknEngineConfig config, Pool<Jedis> jedisPool,
            int threads, EngineGraknTxFactory factory, LockProvider distributedLockClient,
            MetricRegistry metricRegistry) {
        Consumer<Task> consumer = new RedisTaskQueueConsumer(this, engineId, config,
                RedisCountStorage.create(jedisPool, metricRegistry), metricRegistry, factory,
                distributedLockClient);
        LOG.info("Running queue consumer with {} execution threads", threads);
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("redisq-task-manager-%d").build();
        this.redisq = new RedisqBuilder<Task>()
                .setJedisPool(jedisPool)
                .setName(QUEUE_NAME)
                .setConsumer(consumer)
                .setMetricRegistry(metricRegistry)
                .setThreadPool(Executors.newFixedThreadPool(threads, namedThreadFactory))
                .setDocumentClass(Task.class)
                .createRedisq();
        this.taskStorage = RedisTaskStorage.create(redisq, metricRegistry);
    }

    @Override
    public void close() {
        LOG.info("Closing task manager");
        try {
            redisq.close();
        } catch (InterruptedException e) {
            LOG.error("Interrupted while closing queue", e);
        }
    }

    @Override
    public CompletableFuture<Void> start() {
        return CompletableFuture
                .runAsync(redisq::startConsumer)
                .exceptionally(e -> {
                    close();
                    throw new RuntimeException("Failed to initialise subscription");
                });
    }


    @Override
    public void stopTask(TaskId id) {
        // NOOP
        // TODO Implement this
    }

    @Override
    public RedisTaskStorage storage() {
        return taskStorage;
    }

    @Override
    public void addTask(TaskState taskState, TaskConfiguration configuration) {
        Task task = Task.builder()
                .setTaskConfiguration(configuration)
                .setTaskState(taskState).build();
        redisq.push(task);
    }

    @Override
    public void runTask(TaskState taskState, TaskConfiguration configuration) {
        Task task = Task.builder()
                .setTaskConfiguration(configuration)
                .setTaskState(taskState).build();
        try {
            redisq.pushAndWait(task, 5, TimeUnit.MINUTES);
        } catch (WaitException e) {
            throw new RuntimeException("Could not run task", e);
        }
    }

    public Future<Void> subscribeToTask(TaskId taskId)
            throws StateFutureInitializationException, ExecutionException, InterruptedException {
        return redisq
                .getFutureForDocumentStateWait(ImmutableSet.of(DONE, FAILED), taskId.getValue(),
                        TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    public void waitForTask(TaskId taskId)
            throws StateFutureInitializationException, ExecutionException, InterruptedException {
        redisq.getFutureForDocumentStateWait(ImmutableSet.of(DONE, FAILED), taskId.getValue(),
                TIMEOUT_SECONDS, TimeUnit.SECONDS).get();
    }

    public void waitForTask(TaskId taskId, long timeout, TimeUnit timeUnit)
            throws StateFutureInitializationException, ExecutionException, InterruptedException, TimeoutException {
        redisq.getFutureForDocumentStateWait(ImmutableSet.of(DONE, FAILED), taskId.getValue(),
                TIMEOUT_SECONDS, TimeUnit.SECONDS).get(timeout, timeUnit);
    }

    public Redisq getQueue() {
        return redisq;
    }
}
