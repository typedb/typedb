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

import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.TaskId;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.lock.ZookeeperLock;
import ai.grakn.engine.tasks.ExternalOffsetStorage;
import ai.grakn.engine.tasks.TaskConfiguration;
import ai.grakn.engine.tasks.TaskManager;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.engine.tasks.connection.ZookeeperConnection;
import ai.grakn.engine.tasks.storage.TaskStateZookeeperStore;
import ai.grakn.engine.util.EngineID;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static ai.grakn.engine.tasks.config.ConfigHelper.kafkaConsumer;
import static ai.grakn.engine.tasks.config.ConfigHelper.kafkaProducer;
import static ai.grakn.engine.tasks.manager.ExternalStorageRebalancer.rebalanceListener;
import static ai.grakn.engine.util.ExceptionWrapper.noThrow;
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
    private final static String TASK_RUNNER_THREAD_POOL_NAME = "task-runner-pool-%s";
    private final static int TIME_UNTIL_BACKOFF = 60_000;
    private final static String TASKS_STOPPED = "/stopped/%s";
    private final static String TASKS_STOPPED_PREFIX = "/stopped";

    private final Producer<TaskState, TaskConfiguration> producer;
    private final ZookeeperConnection zookeeper;
    private final TaskStateStorage storage;
    private final PathChildrenCache stoppedTasks;
    private final ExternalOffsetStorage offsetStorage;
    private final GraknEngineConfig config;

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
    public SingleQueueTaskManager(EngineID engineId, GraknEngineConfig config) {
        this.config = config;
        this.zookeeper = new ZookeeperConnection(config);
        this.storage = new TaskStateZookeeperStore(zookeeper);
        this.offsetStorage = new ExternalOffsetStorage(zookeeper);

        //TODO check that the number of partitions is at least the capacity
        //TODO only pass necessary Kafka properties
        this.producer = kafkaProducer(config.getProperties());

        // Create thread pool for the task runners
        ThreadFactory taskRunnerPoolFactory = new ThreadFactoryBuilder()
                .setNameFormat(TASK_RUNNER_THREAD_POOL_NAME)
                .build();

        int capacity = config.getAvailableThreads();

        this.taskRunnerThreadPool = newFixedThreadPool(capacity * 2, taskRunnerPoolFactory);

        // Create and start the task runners
        Set<SingleQueueTaskRunner> highPriorityTaskRunners = generate(() -> newTaskRunner(engineId, TaskState.Priority.HIGH.queue())).limit(capacity).collect(toSet());
        Set<SingleQueueTaskRunner> lowPriorityTaskRunners = generate(() -> newTaskRunner(engineId, TaskState.Priority.LOW.queue())).limit(capacity).collect(toSet());

        this.taskRunners = Stream.concat(highPriorityTaskRunners.stream(), lowPriorityTaskRunners.stream()).collect(toSet());
        this.taskRunners.forEach(taskRunnerThreadPool::submit);

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

        LockProvider.instantiate((lockPath, existingLock) -> new ZookeeperLock(zookeeper, lockPath));

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

        // stop zookeeper connection
        noThrow(zookeeper::close, "Error waiting for zookeeper connection to close");

        LockProvider.clear();

        LOG.debug("TaskManager closed");
    }

    /**
     * Stop a task from running.
     */
    @Override
    public void stopTask(TaskId id) {
        byte[] serializedId = id.getValue().getBytes(zkCharset);
        try {
            zookeeper.connection().create().creatingParentsIfNeeded().forPath(String.format(TASKS_STOPPED, id), serializedId);
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
     * Get a new kafka consumer listening on the given topic
     */
    private Consumer<TaskState, TaskConfiguration> newConsumer(String topic){
        Properties properties = config.getProperties();  // TODO: Only pass necessary kafka properties
        Consumer<TaskState, TaskConfiguration> consumer = kafkaConsumer("task-runners-" + topic, properties);
        consumer.subscribe(ImmutableList.of(topic), rebalanceListener(consumer, offsetStorage));
        return consumer;
    }

    /**
     * Check in Zookeeper whether the task has been stopped.
     * @param taskId the task ID to look up in Zookeeper
     * @return true if the task has been marked stopped
     */
    boolean isTaskMarkedStopped(TaskId taskId) {
        return stoppedTasks.getCurrentData(String.format(TASKS_STOPPED, taskId)) != null;
    }

    /**
     * Serialize and send the given task to the given kafka queue
     * @param taskState Task to send to kafka
     * @param configuration Configuration of the given task
     */
    @Override
    public void addTask(TaskState taskState, TaskConfiguration configuration){
        producer.send(new ProducerRecord<>(taskState.priority().queue(), taskState, configuration));
        producer.flush();
    }

    /**
     * Create a new instance of {@link SingleQueueTaskRunner} with the configured {@link #storage}}
     * and {@link #zookeeper} connection.
     * @param engineId Identifier of the engine on which this taskrunner is running
     * @return New instance of a SingleQueueTaskRunner
     */
    private SingleQueueTaskRunner newTaskRunner(EngineID engineId, String priority){
        return new SingleQueueTaskRunner(this, engineId, offsetStorage, TIME_UNTIL_BACKOFF, newConsumer(priority));
    }
}
