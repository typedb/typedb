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
import ai.grakn.engine.tasks.config.ConfigHelper;
import ai.grakn.engine.tasks.manager.ExternalStorageRebalancer;
import ai.grakn.engine.tasks.manager.ZookeeperConnection;
import ai.grakn.engine.tasks.storage.TaskStateZookeeperStore;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.engine.util.EngineID;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static ai.grakn.engine.tasks.config.KafkaTerms.TASK_RUNNER_GROUP;
import static ai.grakn.engine.tasks.config.KafkaTerms.NEW_TASKS_TOPIC;
import static ai.grakn.engine.tasks.config.ConfigHelper.client;
import static ai.grakn.engine.util.ExceptionWrapper.noThrow;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;

/**
 * {@link TaskManager} implementation that operates using a single Kafka queue and controls the
 * lifecycle {@link SingleQueueTaskManager}
 *
 * @author aelred, alexandrorth
 */
public class SingleQueueTaskManager implements TaskManager {

    private final static Logger LOG = LoggerFactory.getLogger(SingleQueueTaskManager.class);
    private final static String ENGINE_IDENTIFIER = EngineID.getInstance().id();
    private final static String TASK_RUNNER_THREAD_NAME = "task-runner-";
    private final static String TASK_RUNNER_THREAD_POOL_NAME = "task-runner-pool-%s";

    //TODO make these two classes with with the TaskId object by implementing a serializer
    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> consumer;
    private final ZookeeperConnection zookeeper;
    private final TaskStateStorage storage;
    private final FailoverElector failover;

    private ExecutorService taskRunnerThreadPool;
    private SingleQueueTaskRunner taskRunner;

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

        this.failover = new FailoverElector(ENGINE_IDENTIFIER, zookeeper, storage);

        this.producer = ConfigHelper.kafkaProducer();
        this.consumer = ConfigHelper.kafkaConsumer(TASK_RUNNER_GROUP);

        createTaskRunner();
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

        // close task runner
        noThrow(taskRunner::close, "Error shutting down TaskRunner");

        // close the kafka consumer used in the task runner
        noThrow(consumer::close, "Error closing the new tasks consumer");

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
        producer.send(new ProducerRecord<>(NEW_TASKS_TOPIC, taskState.getId().getValue(), TaskState.serialize(taskState)));
        //TODO do we need to flush here? when you figure it out write in javadoc
        producer.flush();
    }

    /**
     * Stop a task from running.
     */
    @Override
    public TaskManager stopTask(TaskId id, String requesterName) {
        throw new UnsupportedOperationException("SingleQueueTaskManager does not support stopping tasks.");
    }

    /**
     * Access the storage that this instance of TaskManager uses.
     * @return A TaskStateStorage object
     */
    @Override
    public TaskStateStorage storage() {
        return storage;
    }

    private void createTaskRunner(){
        ConsumerRebalanceListener listener = new ExternalStorageRebalancer(consumer, zookeeper, this.getClass().getSimpleName());
        this.consumer.subscribe(singletonList(NEW_TASKS_TOPIC), listener);

        int capacity = ConfigProperties.getInstance().getAvailableThreads();
        ThreadFactory taskRunnerPoolFactory = new ThreadFactoryBuilder().setNameFormat(TASK_RUNNER_THREAD_POOL_NAME).build();

        this.taskRunnerThreadPool = new ThreadPoolExecutor(capacity, capacity, 0L,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(capacity), taskRunnerPoolFactory);
        this.taskRunner = new SingleQueueTaskRunner(storage, consumer, taskRunnerThreadPool);
        Thread taskRunnerThread = new Thread(taskRunner, TASK_RUNNER_THREAD_NAME);
        taskRunnerThread.setUncaughtExceptionHandler(new TaskRunnerResurrection());
        taskRunnerThread.start();
    }

    /**
     * Implementation of UncaughtExceptionHandler that will restart the TaskRunner in a new thread
     * if it throws any unchecked exception
     *
     * @author alexandraorth
     * TODO This method needs some serious testing
     */
    private class TaskRunnerResurrection implements Thread.UncaughtExceptionHandler {

        public void uncaughtException(Thread paramThread, Throwable paramThrowable) {
            LOG.debug(format("TaskRunner [%s] threw an exception. Will attempt to close and reopen. Exception is: %n [%s]",
                    paramThread.getName(), getFullStackTrace(paramThrowable)));

            // close the task runner
            noThrow(taskRunner::close, "Error shutting down TaskRunner");

            // close the thread pool
            noThrow(taskRunnerThreadPool::shutdown, "Error closing task runner thread pool");
            noThrow(() -> taskRunnerThreadPool.awaitTermination(1, TimeUnit.MINUTES),
                    "Error waiting for TaskRunner executor to shutdown.");

            LOG.debug("TaskRunner closed.");

            // re-instantiate task runner
            createTaskRunner();

            LOG.debug("Re-instantiation of TaskRunner completed.");
        }
    }
}
