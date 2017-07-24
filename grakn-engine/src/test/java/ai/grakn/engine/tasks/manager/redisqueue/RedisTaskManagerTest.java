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
import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.engine.TaskStatus.FAILED;
import static ai.grakn.engine.TaskStatus.RUNNING;
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import ai.grakn.engine.lock.ProcessWideLockProvider;
import ai.grakn.engine.tasks.manager.TaskConfiguration;
import ai.grakn.engine.tasks.manager.TaskSchedule;
import ai.grakn.engine.tasks.manager.TaskState;
import ai.grakn.engine.tasks.manager.TaskState.Priority;
import ai.grakn.engine.tasks.mock.ShortExecutionMockTask;
import ai.grakn.engine.util.EngineID;
import ai.grakn.test.GraphContext;
import ai.grakn.util.EmbeddedRedis;
import static ai.grakn.util.REST.Request.COMMIT_LOG_COUNTING;
import static ai.grakn.util.REST.Request.KEYSPACE;
import com.codahale.metrics.MetricRegistry;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNotSame;
import static junit.framework.TestCase.fail;
import mjson.Json;
import org.junit.AfterClass;
import static org.junit.Assert.assertTrue;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisTaskManagerTest {

    public static final Retryer<Boolean> RETRY_STRATEGY = RetryerBuilder.<Boolean>newBuilder()
            .withStopStrategy(StopStrategies.stopAfterAttempt(10))
            .retryIfResult(aBoolean -> false)
            .retryIfExceptionOfType(ai.grakn.exception.GraknBackendException.class)
            .withWaitStrategy(WaitStrategies.exponentialWait(10, 60, TimeUnit.SECONDS))
            .build();
    private static final int PORT = 9899;
    private static final int MAX_TOTAL = 256;
    public static final GraknEngineConfig CONFIG = GraknEngineConfig.create();
    private static final MetricRegistry metricRegistry = new MetricRegistry();
    private static final EngineID engineID = EngineID.of("engineID");
    public static final ProcessWideLockProvider LOCK_PROVIDER = new ProcessWideLockProvider();

    private static JedisPool jedisPool;
    private static EngineGraknGraphFactory engineGraknGraphFactory;

    private static ExecutorService executor;
    private static RedisTaskManager taskManager;

    @ClassRule
    public static final GraphContext graph = GraphContext.empty();

    @BeforeClass
    public static void setupClass() {
        EmbeddedRedis.start(PORT);
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setBlockWhenExhausted(true);
        poolConfig.setMaxTotal(MAX_TOTAL);
        jedisPool = new JedisPool(poolConfig, "localhost", 9899);
        assertFalse(jedisPool.isClosed());
        engineGraknGraphFactory = EngineGraknGraphFactory.createAndLoadSystemOntology(CONFIG.getProperties());
        int nThreads = 2;
        executor = Executors.newFixedThreadPool(nThreads);
        taskManager = new RedisTaskManager(engineID, CONFIG, jedisPool, nThreads, engineGraknGraphFactory, LOCK_PROVIDER, metricRegistry);
        CompletableFuture<Void> cf = taskManager.start();
        cf.join();
    }

    @AfterClass
    public static void tearDownClass() throws InterruptedException {
        taskManager.close();
        executor.awaitTermination(3, TimeUnit.SECONDS);
        jedisPool.close();
        EmbeddedRedis.stop();
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

    @Test(expected = RetryException.class)
    public void whenNotAddingTask_TastStateIsNotRetrievable() throws ExecutionException, RetryException {
        TaskState state = TaskState.of(ShortExecutionMockTask.class, RedisTaskManagerTest.class.getName(), TaskSchedule.now(), Priority.LOW);
        RETRY_STRATEGY.call(() -> taskManager.storage().getState(state.getId()) != null);
        assertNotSame(COMPLETED, taskManager.storage().getState(state.getId()).status());
    }

    @Test
    public void whenConfigurationEmpty_TaskEventuallyFailed() throws ExecutionException, RetryException {
        TaskState state = TaskState.of(ShortExecutionMockTask.class, RedisTaskManagerTest.class.getName(), TaskSchedule.now(), Priority.LOW);
        taskManager.addTask(state, TaskConfiguration.of(Json.object()));
        RETRY_STRATEGY.call(() -> taskManager.storage().getState(state.getId()).status() == FAILED);
        assertEquals(FAILED, taskManager.storage().getState(state.getId()).status());
    }

    @Test
    public void whenSending100Tasks_AllTaskStatesRetrievable() throws ExecutionException, RetryException {
        ArrayList<TaskState> states = new ArrayList<>();
        for(int i = 0; i < 10; i++) {
            TaskId generate = TaskId.generate();
            TaskState state = TaskState.of(ShortExecutionMockTask.class, RedisTaskManagerTest.class.getName(), TaskSchedule.now(), Priority.LOW);
            states.add(state);
            taskManager.addTask(state, testConfig(generate));
        }
        states.forEach(state -> {
            try {
                RETRY_STRATEGY.call(() -> taskManager.storage().getState(state.getId()) != null);
            } catch (Exception e) {
                fail("Failed to retrieve task in time");
            }
            assertTrue("Task retrieved but with unexpected state", ImmutableSet.of(COMPLETED, RUNNING).contains(taskManager.storage().getState(state.getId()).status()));
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