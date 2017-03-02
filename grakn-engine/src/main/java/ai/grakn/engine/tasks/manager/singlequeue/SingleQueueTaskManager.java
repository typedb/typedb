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

import ai.grakn.engine.tasks.TaskId;
import ai.grakn.engine.tasks.TaskManager;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.engine.tasks.manager.ZookeeperConnection;
import ai.grakn.engine.tasks.storage.TaskStateZookeeperStore;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.engine.util.EngineID;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static ai.grakn.engine.TaskStatus.STOPPED;
import static ai.grakn.engine.tasks.config.ConfigHelper.client;
import static ai.grakn.engine.tasks.config.ConfigHelper.kafkaProducer;
import static ai.grakn.engine.tasks.config.KafkaTerms.NEW_TASKS_TOPIC;
import static ai.grakn.engine.util.ExceptionWrapper.noThrow;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.generate;

/**
 * {@link TaskManager} implementation that operates using a single Kafka queue and controls the
 * lifecycle {@link SingleQueueTaskManager}
 *
 * @author aelred, alexandrorth
 */
public class SingleQueueTaskManager implements TaskManager {

    private final static Logger LOG = LoggerFactory.getLogger(SingleQueueTaskManager.class);
    private final static String ENGINE_IDENTIFIER = EngineID.getInstance().id();
    private final static String TASK_RUNNER_THREAD_POOL_NAME = "task-runner-pool-%s";
    private final static int CAPACITY = ConfigProperties.getInstance().getAvailableThreads();

    private final Producer<TaskId, TaskState> producer;
    private final ZookeeperConnection zookeeper;
    private final TaskStateStorage storage;
    private final FailoverElector failover;

    private Set<SingleQueueTaskRunner> taskRunners;
    private ExecutorService taskRunnerThreadPool;

    /**
     * Create a {@link SingleQueueTaskManager}
     *
     * The SingleQueueTaskManager implementation must:
     *  + Instantiate a connection to zookeeper
     *  + Configure and instance of TaskStateStorage
     *  + Create and run an instance of SingleQueueTaskRunner
     *  + Add oneself to the leader elector by instantiating failoverelector
     */
    public SingleQueueTaskManager(){
        this.zookeeper = new ZookeeperConnection(client());
        this.storage = new TaskStateZookeeperStore(zookeeper);

        //TODO check that the number of partitions is at least the capacity
        //TODO Single queue task manager should have its own impl of failover
        this.failover = new FailoverElector(ENGINE_IDENTIFIER, zookeeper, storage);
        this.producer = kafkaProducer();

        // Create thread pool for the task runners
        ThreadFactory taskRunnerPoolFactory = new ThreadFactoryBuilder()
                .setNameFormat(TASK_RUNNER_THREAD_POOL_NAME)
                .build();
        this.taskRunnerThreadPool = newFixedThreadPool(CAPACITY, taskRunnerPoolFactory);

        // Create and start the task runners
        this.taskRunners = generate(this::newTaskRunner).limit(CAPACITY).collect(toSet());
        this.taskRunners.forEach(taskRunnerThreadPool::submit);

        LOG.debug("TaskManager started");
    }

    /**
     * Close the {@link SingleQueueTaskRunner} and . Any errors that occur should not prevent the
     * subsequent ones from executing.
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        LOG.debug("Closing SingleQueueTaskManager");

        // close kafka producer
        noThrow(producer::close, "Error shutting down producer in TaskManager");

        // Close all the task runners
        for(SingleQueueTaskRunner taskRunner:taskRunners) {
            noThrow(taskRunner::close, "Error shutting down TaskRunner");
        }

        // close the thread pool and wait for shutdown
        noThrow(taskRunnerThreadPool::shutdown, "Error closing task runner thread pool");
        noThrow(() -> taskRunnerThreadPool.awaitTermination(1, TimeUnit.MINUTES),
                "Error waiting for TaskRunner executor to shutdown.");

        // remove this engine from leadership election
        noThrow(failover::renounce, "Error renouncing participation in leadership election");

        // stop zookeeper connection
        noThrow(zookeeper::close, "Error waiting for zookeeper connection to close");

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
    public void stopTask(TaskId id, String requesterName) {
        // TODO: Make only one call to storage if possible
        if (!storage.containsTask(id)) {
            // TODO: Figure out a nicer way than all these nulls...
            TaskState task = TaskState.of(null, null, null, null, id).status(STOPPED);
            storage.newState(task);
        } else {
            taskRunners.forEach(taskRunner -> taskRunner.stopTask(id));
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
     * Create a new instance of {@link SingleQueueTaskRunner} with the configured {@link #storage}}
     * and {@link #zookeeper} connection.
     * @return New instance of a SingleQueueTaskRunner
     */
    private SingleQueueTaskRunner newTaskRunner(){
        return new SingleQueueTaskRunner(storage, zookeeper);
    }
}
