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

package ai.grakn.engine.tasks.manager.multiqueue;

import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.TaskId;
import ai.grakn.engine.tasks.ExternalOffsetStorage;
import ai.grakn.engine.tasks.TaskCheckpoint;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.engine.tasks.manager.ZookeeperConnection;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.util.EngineID;
import ai.grakn.exception.EngineStorageException;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static ai.grakn.engine.TaskStatus.SCHEDULED;
import static ai.grakn.engine.tasks.config.ConfigHelper.kafkaConsumer;
import static ai.grakn.engine.tasks.config.KafkaTerms.TASK_RUNNER_GROUP;
import static ai.grakn.engine.tasks.config.KafkaTerms.WORK_QUEUE_TOPIC;
import static ai.grakn.engine.tasks.config.ZookeeperPaths.SINGLE_ENGINE_WATCH_PATH;
import static ai.grakn.engine.tasks.manager.ExternalStorageRebalancer.rebalanceListener;
import static ai.grakn.engine.GraknEngineConfig.TASKRUNNER_POLLING_FREQ;
import static ai.grakn.engine.util.ExceptionWrapper.noThrow;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;

/**
 * <p>
 *      Picks up tasks from the work queue, runs them and marks them as completed or failed.
 * </p>
 *
 * <p>
 *     Runs tasks in a pool. The size of this pool is configurable in the properties file.
 *     Controls marking the state of running TaskRunner in Zookeeper.
 * </p>
 *
 * @author Denis Lobanov, alexandraorth
 */
public class MultiQueueTaskRunner implements Runnable, AutoCloseable {
    private final static Logger LOG = LoggerFactory.getLogger(MultiQueueTaskRunner.class);
    private final static GraknEngineConfig properties = GraknEngineConfig.getInstance();

    private final static int POLLING_FREQUENCY = properties.getPropertyAsInt(TASKRUNNER_POLLING_FREQ);
    private final EngineID engineId;

    private final Set<TaskId> runningTasks = new HashSet<>();
    private final TaskStateStorage storage;
    private final ZookeeperConnection connection;
    private final CountDownLatch shutdownLatch;

    private final ExecutorService executor;
    private final int executorSize;
    private final AtomicInteger acceptedTasks = new AtomicInteger(0);
    private final Consumer<TaskId, TaskState> consumer;

    public MultiQueueTaskRunner(EngineID engineId, TaskStateStorage storage, ZookeeperConnection connection) {
        this.engineId = engineId;
        this.storage = storage;
        this.connection = connection;

        // Create the consumer
        consumer = kafkaConsumer(TASK_RUNNER_GROUP);

        // Configure callback for a Kafka rebalance
        consumer.subscribe(singletonList(WORK_QUEUE_TOPIC), rebalanceListener(consumer, new ExternalOffsetStorage(connection)));

        // Create initial entries in ZK for TaskFailover to watch.
        registerAsRunning();

        // Instantiate the executor where tasks will run
        // executorSize is the maximum executor queue size
        int numberAvailableThreads = properties.getAvailableThreads();
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("task-runner-pool-%d").build();
        executor = newFixedThreadPool(numberAvailableThreads, namedThreadFactory);
        executorSize = numberAvailableThreads * 4;

        shutdownLatch = new CountDownLatch(1);

        LOG.info("TaskRunner started");
    }

    /**
     * Start the main loop, this will block until a call to close() that wakes up the consumer.
     *
     * The only way to exit this loop without throwing an exception is by calling consumer.wakeup()
     *
     * We do not want to catch any exceptions here. The caller of this TaskRunner should handle the case
     * where an exception is thrown. It is recommended to register the TaskRunner thread with a UncaughtExceptionHandler.
     * The catch(Throwable t) here is merely for logging purposes. You will notice that the exception is re-thrown.
     */
    public void run()  {
        try {
            while (true) {
                ConsumerRecords<TaskId, TaskState> records = consumer.poll(POLLING_FREQUENCY);

                long startTime = System.currentTimeMillis();
                for(ConsumerRecord<TaskId, TaskState> record: records) {

                    // If TaskRunner capacity full commit offset as current record and exit
                    if(acceptedTasks.get() >= executorSize) {
                        acknowledgeRecordSeen(record);
                    } else {
                        processAndAcknowledgeProcessed(record);
                    }
                }

                LOG.debug(format("Took [%s] ms to process [%s] records in taskrunner",
                        System.currentTimeMillis() - startTime, records.count()));
            }
        } catch (WakeupException e) {
            LOG.debug("TaskRunner exiting, woken up.");
        } catch (Throwable throwable){
            LOG.error("Error in TaskRunner poll " + throwable.getMessage());

            // re-throw the exception
            throw throwable;
        } finally {
            noThrow(consumer::close, "Exception while closing consumer in TaskRunner");
            noThrow(shutdownLatch::countDown, "Exception while counting down close latch in TaskRunner");

            LOG.debug("TaskRunner run() end");
        }
    }

    /**
     * Stop the main loop, causing run() to exit.
     *
     * noThrow() functions used here so that if an error occurs during execution of a
     * certain step, the subsequent stops continue to execute.
     */
    public void close() {
        // Stop execution of kafka consumer
        noThrow(consumer::wakeup, "Could not wake up task runner thread.");

        // Wait for the shutdown latch to complete
        noThrow(shutdownLatch::await, "Error waiting for TaskRunner consumer to exit");

        // Interrupt all currently running threads - these will be re-allocated to another Engine.
        noThrow(executor::shutdown, "Could not shutdown TaskRunner executor.");
        noThrow(() -> executor.awaitTermination(1, TimeUnit.MINUTES), "Error waiting for TaskRunner executor to shutdown.");

        LOG.debug("TaskRunner stopped");
    }

    /**
     * Add a single record to the threadpool if it has been marked as SCHEDULED. At the end of
     * the method acknowledge the record has been read to the consumer.
     *
     * @param record The record to execute.
     */
    private void processAndAcknowledgeProcessed(ConsumerRecord<TaskId, TaskState> record) {
        try {
            LOG.debug(format("Received [%s], currently running: %s has: %s allowed: %s",
                record.key(), getRunningTasksCount(), acceptedTasks.get(), executorSize));

            // Get up-to-date state from the storage
            TaskState state = storage.getState(record.key());

            // If the task is scheduled, run it
            if (state.status() == SCHEDULED) {

                // Mark as RUNNING and update task & runner states.
                storage.updateState(state.markRunning(engineId));
                acceptedTasks.incrementAndGet();

                // Submit to executor
                executor.execute(() -> executeTask(state));
            } else {
                LOG.debug(format("Will not run [%s] because status: [%s]", record.key(), state.status()));
            }
        } catch (EngineStorageException e){
            LOG.error(format("Cant run [%s] because state was not found in storage", record.key()));
        } finally {
            // Acknowledge that the TaskRunner has processed this record
            acknowledgeRecordProcessed(record);
        }
    }

    /**
     * Instantiate a BackgroundTask object and run it, catching any thrown Exceptions.
     * @param state TaskState for task @id.
     */
    private void executeTask(TaskState state) {
        LOG.debug("Executing task " + state.getId());

        try {
            // Should add running task here, so it always gets removed in the finally
            addRunningTask(state.getId());

            // Instantiate task.
            BackgroundTask task = state.taskClass().newInstance();

            // Resume task from the checkpoint, if it exists. Otherwise run from the beginning.
            if(state.checkpoint() != null){
                task.resume(saveCheckpoint(state), state.checkpoint());
            } else {
                task.start(saveCheckpoint(state), state.configuration());
            }

            // remove the configuration and mark as COMPLETED
            state.markCompleted();
        } catch(Throwable throwable) {
            state.markFailed(throwable);
            LOG.error("Failed task - "+state.getId()+": "+getFullStackTrace(throwable));
        } finally {
            storage.updateState(state);
            removeRunningTask(state.getId());
            acceptedTasks.decrementAndGet();
            LOG.debug("Finished executing task - " + state.getId());
        }
    }

    /**
     * Persists a Background Task's checkpoint to ZK and graph.
     * @param taskState task to update in storage
     * @return A Consumer<String> function that can be called by the background task on demand to save its checkpoint.
     */
    private java.util.function.Consumer<TaskCheckpoint> saveCheckpoint(TaskState taskState) {
        return checkpoint -> storage.updateState(taskState.checkpoint(checkpoint));
    }

    private void registerAsRunning() {
        try {
            if (connection.connection().checkExists().forPath(format(SINGLE_ENGINE_WATCH_PATH, engineId.value())) == null) {
                connection.connection().create()
                        .creatingParentContainersIfNeeded()
                        .withMode(CreateMode.EPHEMERAL).forPath(format(SINGLE_ENGINE_WATCH_PATH, engineId.value()));
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception exception){
            throw new RuntimeException("Could not create Zookeeper paths in TaskRunner");
        }

        LOG.debug("Registered TaskRunner");
    }

    private synchronized int getRunningTasksCount() {
        return runningTasks.size();
    }

    private synchronized void addRunningTask(TaskId id) {
        runningTasks.add(id);
    }

    private synchronized void removeRunningTask(TaskId id) {
        runningTasks.remove(id);
    }

    /**
     * Instruct kafka to read from the current record
     * @param record The record to read from
     */
    private void acknowledgeRecordSeen(ConsumerRecord record){
        commitOffset(record, record.offset());
    }

    /**
     * Instruct kafka to read from the next record
     * @param record The record to read from
     */
    private void acknowledgeRecordProcessed(ConsumerRecord record){
        commitOffset(record, record.offset() + 1);
    }

    /**
     * Commit the given offset for the partition & topic the given record belongs to
     * @param record Record from which to extract partition and topic
     * @param offset Offset to commit
     */
    private void commitOffset(ConsumerRecord record, long offset) {
        consumer.seek(new TopicPartition(record.topic(), record.partition()), offset);
        consumer.commitSync();
    }
}
