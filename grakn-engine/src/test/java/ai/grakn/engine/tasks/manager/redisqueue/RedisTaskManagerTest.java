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
import ai.grakn.engine.lock.JedisLockProvider;
import ai.grakn.engine.lock.ProcessWideLockProvider;
import ai.grakn.engine.postprocessing.PostProcessor;
import ai.grakn.engine.tasks.manager.TaskConfiguration;
import ai.grakn.engine.tasks.manager.TaskSchedule;
import ai.grakn.engine.tasks.manager.TaskState;
import ai.grakn.engine.tasks.manager.TaskState.Priority;
import ai.grakn.engine.tasks.mock.ShortExecutionMockTask;
import ai.grakn.engine.util.EngineID;
import ai.grakn.redisq.exceptions.StateFutureInitializationException;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.test.rule.InMemoryRedisContext;
import com.codahale.metrics.MetricRegistry;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.collect.ImmutableSet;
import mjson.Json;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.engine.TaskStatus.FAILED;
import static ai.grakn.engine.TaskStatus.RUNNING;
import static ai.grakn.util.REST.Request.COMMIT_LOG_COUNTING;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertTrue;

public class RedisTaskManagerTest {

    public static final Retryer<Boolean> RETRY_STRATEGY = RetryerBuilder.<Boolean>newBuilder()
            .withStopStrategy(StopStrategies.stopAfterAttempt(10))
            .retryIfResult(aBoolean -> false)
            .retryIfExceptionOfType(ai.grakn.exception.GraknBackendException.class)
            .withWaitStrategy(WaitStrategies.exponentialWait(10, 60, TimeUnit.SECONDS))
            .build();
    private static final int MAX_TOTAL = 256;
    public static final GraknEngineConfig CONFIG = GraknEngineConfig.create();
    private static final MetricRegistry metricRegistry = new MetricRegistry();
    private static final EngineID engineID = EngineID.of("engineID");
    public static final ProcessWideLockProvider LOCK_PROVIDER = new ProcessWideLockProvider();

    private static JedisPool jedisPool;
    private static EngineGraknTxFactory engineGraknTxFactory;

    private static ExecutorService executor;
    private static RedisTaskManager taskManager;

    @ClassRule
    public static final SampleKBContext sampleKB = SampleKBContext.empty();

    @ClassRule
    public static InMemoryRedisContext inMemoryRedisContext = InMemoryRedisContext.create();

    @BeforeClass
    public static void setupClass() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setBlockWhenExhausted(true);
        poolConfig.setMaxTotal(MAX_TOTAL);
        jedisPool = inMemoryRedisContext.jedisPool(poolConfig);
        assertFalse(jedisPool.isClosed());
        JedisLockProvider lockProvider = new JedisLockProvider(jedisPool);
        engineGraknTxFactory = EngineGraknTxFactory.createAndLoadSystemSchema(lockProvider, CONFIG.getProperties());
        int nThreads = 2;
        executor = Executors.newFixedThreadPool(nThreads);
        PostProcessor postProcessor = PostProcessor.create(CONFIG, jedisPool, engineGraknTxFactory, LOCK_PROVIDER, metricRegistry);
        taskManager = new RedisTaskManager(engineID, CONFIG, jedisPool, nThreads, engineGraknTxFactory, metricRegistry, postProcessor);
        CompletableFuture<Void> cf = taskManager.start();
        cf.join();
    }

    @AfterClass
    public static void tearDownClass() throws InterruptedException {
        taskManager.close();
        executor.awaitTermination(3, TimeUnit.SECONDS);
        jedisPool.close();
    }

    @Ignore // TODO: Fix (Bug #16193)
    @Test
    public void whenAddingTask_TaskStateIsRetrievable() throws ExecutionException, RetryException {
        TaskId generate = TaskId.generate();
        TaskState state = TaskState.of(ShortExecutionMockTask.class, RedisTaskManagerTest.class.getName(), TaskSchedule.now(), Priority.LOW);
        taskManager.addTask(state, testConfig(generate));
        RETRY_STRATEGY.call(() -> taskManager.storage().getState(state.getId()) != null);
        assertEquals(COMPLETED, taskManager.storage().getState(state.getId()).status());
    }

    @Test(expected = TimeoutException.class)
    public void whenNotAddingTask_TastStateIsNotRetrievable()
            throws ExecutionException, RetryException, StateFutureInitializationException, InterruptedException, TimeoutException {
        TaskState state = TaskState.of(ShortExecutionMockTask.class, RedisTaskManagerTest.class.getName(), TaskSchedule.now(), Priority.LOW);
        taskManager.waitForTask(state.getId(), 3, TimeUnit.SECONDS);
    }

    @Test
    public void whenConfigurationEmpty_TaskEventuallyFailed()
            throws ExecutionException, RetryException, InterruptedException, StateFutureInitializationException, TimeoutException {
        TaskState state = TaskState.of(ShortExecutionMockTask.class, RedisTaskManagerTest.class.getName(), TaskSchedule.now(), Priority.LOW);
        Future<Void> s = taskManager.subscribeToTask(state.getId());
        taskManager.addTask(state, TaskConfiguration.of(Json.object()));
        s.get();
        assertEquals(FAILED, taskManager.storage().getState(state.getId()).status());
    }

    @Test
    public void whenSending10Tasks_AllTaskStatesRetrievable()
            throws ExecutionException, RetryException, StateFutureInitializationException, InterruptedException {
        Map<TaskId, Future<Void>> states = new HashMap<>();

        for(int i = 0; i < 10; i++) {
            TaskId generate = TaskId.generate();
            TaskState state = TaskState.of(ShortExecutionMockTask.class, RedisTaskManagerTest.class.getName(), TaskSchedule.now(), Priority.LOW);
            TaskId id = state.getId();
            states.put(id, taskManager.subscribeToTask(id));
            taskManager.addTask(state, testConfig(generate));
        }

        states.forEach((id, state) -> {
            try {
                System.out.println("Waiting for " + id);
                state.get();
            } catch (Exception e) {
                fail("Failed to retrieve task in time");
            }
            assertTrue("Task retrieved but with unexpected state " + taskManager.storage().getState(id).status(), ImmutableSet.of(COMPLETED, RUNNING).contains(taskManager.storage().getState(id).status()));
        });
    }

    private TaskConfiguration testConfig(TaskId generate) {
        return TaskConfiguration.of(Json.object(
                KEYSPACE, "keyspace",
                COMMIT_LOG_COUNTING, 3,
                "id", generate.getValue()
        ));
    }
}