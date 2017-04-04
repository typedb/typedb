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

package ai.grakn.engine.tasks.manager.singlequeue;

import ai.grakn.engine.TaskId;
import ai.grakn.engine.cache.EngineCacheDistributed;
import ai.grakn.engine.cache.EngineCacheProvider;
import ai.grakn.engine.tasks.ExternalOffsetStorage;
import ai.grakn.engine.tasks.TaskManager;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.engine.tasks.manager.ZookeeperConnection;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.util.EngineID;
import com.google.common.collect.ImmutableList;
import com.google.common.base.Charsets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static ai.grakn.engine.tasks.config.ConfigHelper.kafkaConsumer;
import static ai.grakn.engine.tasks.config.ConfigHelper.kafkaProducer;
import static ai.grakn.engine.tasks.config.KafkaTerms.NEW_TASKS_TOPIC;
import static ai.grakn.engine.tasks.config.KafkaTerms.TASK_RUNNER_GROUP;
import static ai.grakn.engine.tasks.config.ZookeeperPaths.SINGLE_ENGINE_WATCH_PATH;
import static ai.grakn.engine.tasks.manager.ExternalStorageRebalancer.rebalanceListener;
import static ai.grakn.engine.tasks.config.ZookeeperPaths.TASKS_STOPPED;
import static ai.grakn.engine.tasks.config.ZookeeperPaths.TASKS_STOPPED_PREFIX;
import static ai.grakn.engine.util.ExceptionWrapper.noThrow;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.generate;
import static org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type.CHILD_ADDED;

/**
 * {@link TaskManager} implementation that operates using a single Kafka queue and controls the
 * lifecycle {@link SingleQueueTaskManager}
 *
 * @author aelred, alexandrorth
 */
public class SingleQueueTaskManager implements TaskManager {

    private final static Logger LOG = LoggerFactory.getLogger(SingleQueueTaskManager.class);
    private final static GraknEngineConfig properties = GraknEngineConfig.getInstance();
    private final static String TASK_RUNNER_THREAD_POOL_NAME = "task-runner-pool-%s";
    private final static int CAPACITY = GraknEngineConfig.getInstance().getAvailableThreads();
    private final static int TIME_UNTIL_BACKOFF = 60_000;

    private final Producer<TaskId, TaskState> producer;
    private final ZookeeperConnection zookeeper;
    private final TaskStateStorage storage;
    private final FailoverElector failover;
    private final PathChildrenCache stoppedTasks;
    private final ExternalOffsetStorage offsetStorage;

    private Set<SingleQueueTaskRunner> taskRunners;
    private ExecutorService taskRunnerThreadPool;

    private Charset zkCharset = Charsets.UTF_8;

    /**
     * Create a {@link SingleQueueTaskManager}
     *
     * The SingleQueueTaskManager implementation must:
     *  + Instantiate a connection to zookeeper
     *  + Configure and instance of TaskStateStorage
     *  + Create and run an instance of SingleQueueTaskRunner
     *  + Add oneself to the leader elector by instantiating failoverelector
     */
    public SingleQueueTaskManager(EngineID engineId) throws Exception {
        this.zookeeper = new ZookeeperConnection();
        this.storage = chooseStorage(properties, zookeeper);
        this.offsetStorage = new ExternalOffsetStorage(zookeeper);

        stoppedTasks = new PathChildrenCache(zookeeper.connection(), TASKS_STOPPED_PREFIX, true);
        stoppedTasks.getListenable().addListener((client, event) -> {
            if (event.getType() == CHILD_ADDED) {
                TaskId id = TaskId.of(new String(event.getData().getData(), zkCharset));
                LOG.debug("Attempting to stop task {}", id);
                taskRunners.forEach(taskRunner -> taskRunner.stopTask(id));
            }
        });
        try {
            stoppedTasks.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        //TODO check that the number of partitions is at least the capacity
        this.failover = new FailoverElector(engineId, zookeeper, storage);
        this.producer = kafkaProducer();

        registerSelfForFailover(engineId, zookeeper);

        // Create thread pool for the task runners
        ThreadFactory taskRunnerPoolFactory = new ThreadFactoryBuilder()
                .setNameFormat(TASK_RUNNER_THREAD_POOL_NAME)
                .build();
        this.taskRunnerThreadPool = newFixedThreadPool(CAPACITY, taskRunnerPoolFactory);

        // Create and start the task runners
        this.taskRunners = generate(() -> newTaskRunner(engineId)).limit(CAPACITY).collect(toSet());
        this.taskRunners.forEach(taskRunnerThreadPool::submit);

        EngineCacheProvider.init(EngineCacheDistributed.init(zookeeper));

        LOG.debug("TaskManager started");
    }

    /**
     * Close the {@link SingleQueueTaskRunner} and . Any errors that occur should not prevent the
     * subsequent ones from executing.
     */
    @Override
    public void close() {
        LOG.debug("Closing SingleQueueTaskManager");

        noThrow(stoppedTasks::close, "Error closing down stop tasks listener");

        // Close all the task runners
        for(SingleQueueTaskRunner taskRunner:taskRunners) {
            noThrow(taskRunner::close, "Error shutting down TaskRunner");
        }

        // close kafka producer
        noThrow(producer::close, "Error shutting down producer in TaskManager");

        // close the thread pool and wait for shutdown
        noThrow(taskRunnerThreadPool::shutdown, "Error closing task runner thread pool");
        noThrow(() -> taskRunnerThreadPool.awaitTermination(1, TimeUnit.MINUTES),
                "Error waiting for TaskRunner executor to shutdown.");

        // remove this engine from leadership election
        noThrow(failover::renounce, "Error renouncing participation in leadership election");

        // stop zookeeper connection
        noThrow(zookeeper::close, "Error waiting for zookeeper connection to close");

        EngineCacheProvider.clearCache();

        LOG.debug("TaskManager closed");
    }

    /**
     * Create an instance of a task based on the given parameters and submit it a Kafka queue.
     * @param taskState Task to execute
     */
    @Override
    public void addTask(TaskState taskState){
        producer.send(new ProducerRecord<>(NEW_TASKS_TOPIC, taskState.getId(), taskState));
        producer.flush();
    }

    /**
     * Stop a task from running.
     */
    @Override
    public void stopTask(TaskId id) {
        byte[] serializedId = id.getValue().getBytes(zkCharset);
        try {
            zookeeper.connection().create().creatingParentsIfNeeded().forPath(format(TASKS_STOPPED, id), serializedId);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Access the storage that this instance of TaskManager uses.
     * @return A TaskStateStorage object
     */
    @Override
    public TaskStateStorage storage() {
        return storage;
    }

    /**
     * Get a new kafka consumer listening on the new tasks topic
     */
    public Consumer<TaskId, TaskState> newConsumer(){
        Consumer<TaskId, TaskState> consumer = kafkaConsumer(TASK_RUNNER_GROUP);
        consumer.subscribe(ImmutableList.of(NEW_TASKS_TOPIC), rebalanceListener(consumer, offsetStorage));
        return consumer;
    }

    /**
     * Check in Zookeeper whether the task has been stopped.
     * @param taskId the task ID to look up in Zookeeper
     * @return true if the task has been marked stopped
     */
    boolean isTaskMarkedStopped(TaskId taskId) {
        return stoppedTasks.getCurrentData(format(TASKS_STOPPED, taskId)) != null;
    }

    /**
     * Create a new instance of {@link SingleQueueTaskRunner} with the configured {@link #storage}}
     * and {@link #zookeeper} connection.
     * @param engineId Identifier of the engine on which this taskrunner is running
     * @return New instance of a SingleQueueTaskRunner
     */
    private SingleQueueTaskRunner newTaskRunner(EngineID engineId){
        return new SingleQueueTaskRunner(this, engineId, offsetStorage, TIME_UNTIL_BACKOFF);
    }

    /**
     * Register this instance of Engine in Zookeeper to monitor its status
     *
     * @param engineId identifier of this instance of engine, which will be registered in Zookeeper
     * @param zookeeper connection to zookeeper
     * @throws Exception when there is an issue contacting or writing to zookeeper
     */
    private void registerSelfForFailover(EngineID engineId, ZookeeperConnection zookeeper) throws Exception {
        zookeeper.connection()
                .create()
                .creatingParentContainersIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(format(SINGLE_ENGINE_WATCH_PATH, engineId.value()));
    }
}
