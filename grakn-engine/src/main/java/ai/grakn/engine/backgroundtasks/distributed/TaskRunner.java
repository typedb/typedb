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
 */

package ai.grakn.engine.backgroundtasks.distributed;

import ai.grakn.engine.backgroundtasks.BackgroundTask;
import ai.grakn.engine.backgroundtasks.TaskStateStorage;
import ai.grakn.engine.backgroundtasks.TaskState;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.engine.util.EngineID;
import ai.grakn.exception.EngineStorageException;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.zookeeper.CreateMode;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.engine.TaskStatus.FAILED;
import static ai.grakn.engine.TaskStatus.RUNNING;
import static ai.grakn.engine.TaskStatus.SCHEDULED;
import static ai.grakn.engine.backgroundtasks.config.ConfigHelper.kafkaConsumer;
import static ai.grakn.engine.backgroundtasks.config.KafkaTerms.TASK_RUNNER_GROUP;
import static ai.grakn.engine.backgroundtasks.config.KafkaTerms.WORK_QUEUE_TOPIC;
import static ai.grakn.engine.backgroundtasks.config.ZookeeperPaths.RUNNERS_STATE;
import static ai.grakn.engine.backgroundtasks.config.ZookeeperPaths.RUNNERS_WATCH;
import static ai.grakn.engine.util.ConfigProperties.TASKRUNNER_POLLING_FREQ;
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
public class TaskRunner implements Runnable, AutoCloseable {
    private final static Logger LOG = LoggerFactory.getLogger(TaskRunner.class);
    private final static ConfigProperties properties = ConfigProperties.getInstance();

    private final static int POLLING_FREQUENCY = properties.getPropertyAsInt(TASKRUNNER_POLLING_FREQ);
    private final static String ENGINE_ID = EngineID.getInstance().id();

    private final Set<String> runningTasks = new HashSet<>();
    private final TaskStateStorage storage;
    private final ZookeeperConnection connection;
    private final CountDownLatch shutdownLatch;

    private final ExecutorService executor;
    private final int executorSize;
    private final AtomicInteger acceptedTasks = new AtomicInteger(0);
    private final KafkaConsumer<String, String> consumer;

    public TaskRunner(TaskStateStorage storage, ZookeeperConnection connection) {
        this.storage = storage;
        this.connection = connection;

        consumer = kafkaConsumer(TASK_RUNNER_GROUP);
        consumer.subscribe(singletonList(WORK_QUEUE_TOPIC), new HandleRebalance());

        // Create initial entries in ZK for TaskFailover to watch.
        registerAsRunning();
        updateOwnState();

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
     */
    public void run()  {
        try {
            while (true) {
                // Poll for new tasks only when we know we have space to accept them.
                processRecords(consumer.poll(POLLING_FREQUENCY));
            }
        } catch (WakeupException e) {
            LOG.debug("TaskRunner exiting, woken up.");
        } catch (Throwable t){
            LOG.error("Error in TaskRunner poll " + getFullStackTrace(t));
        } finally {
            noThrow(consumer::commitSync, "Exception syncing commits while closing in TaskRunner");
            noThrow(consumer::close, "Exception while closing consumer in TaskRunner");
            noThrow(shutdownLatch::countDown, "Exception while counting down close latch in TaskRunner");
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
        noThrow(executor::shutdownNow, "Could not shutdown scheduling service.");

        LOG.debug("TaskRunner stopped");
    }

    private void processRecords(ConsumerRecords<String, String> records) {
        for(ConsumerRecord<String, String> record: records) {
            // Exit loop when TaskRunner capacity full
            if(acceptedTasks.get() >= executorSize) {
                seekAndCommit(new TopicPartition(record.topic(), record.partition()), record.offset());
                break;
            }

            LOG.debug(format("Received [%s] of [%s], currently running: %s has: %s allowed: %s",
                    record.key(), records.count(), getRunningTasksCount(), acceptedTasks.get(), executorSize));

            String id = record.key();
            try {
                // Instead of deserializing TaskState from value, get up-to-date state from the storage
                TaskState state = storage.getState(id);
                if (state.status() != SCHEDULED) {
                    LOG.debug("Cant run this task - " + id + " because\n\t\tstatus: " + state.status());
                    continue;
                }

                // Mark as RUNNING and update task & runner states.
                storage.updateState(state
                    .status(RUNNING)
                    .statusChangedBy(this.getClass().getName())
                    .engineID(ENGINE_ID));

                acceptedTasks.incrementAndGet();
                // Submit to executor
                executor.submit(() -> executeTask(state));

                // Advance offset
                seekAndCommit(new TopicPartition(record.topic(), record.partition()), record.offset() + 1);
            } catch (EngineStorageException e){
                LOG.error("Cant run this task - " + id + " because state was not found in storage");
            }
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
            Class<?> c = Class.forName(state.taskClassName());
            BackgroundTask task = (BackgroundTask) c.newInstance();

            // Resume task from the checkpoint, if it exists. Otherwise run from the beginning.
            if(state.checkpoint() != null){
                task.resume(saveCheckpoint(state), state.checkpoint());
            } else {
                task.start(saveCheckpoint(state), state.configuration());
            }

            storage.updateState(state.status(COMPLETED));
        } catch(Throwable t) {
            storage.updateState(state.status(FAILED));
            LOG.error("Failed task - "+state.getId()+": "+getFullStackTrace(t));
        } finally {
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
    private Consumer<String> saveCheckpoint(TaskState taskState) {
        return checkpoint -> storage.updateState(taskState.checkpoint(checkpoint));
    }

    private void updateOwnState() {
        JSONArray out = new JSONArray();
        out.put(runningTasks);

        try {
            connection.connection().setData().forPath(RUNNERS_STATE+"/"+ ENGINE_ID, out.toString().getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            LOG.error("Could not update TaskRunner taskstorage in ZooKeeper! " + e);
        }
    }

    private void registerAsRunning() {
        try {
            if (connection.connection().checkExists().forPath(RUNNERS_WATCH + "/" + ENGINE_ID) == null) {
                connection.connection().create()
                        .creatingParentContainersIfNeeded()
                        .withMode(CreateMode.EPHEMERAL).forPath(RUNNERS_WATCH + "/" + ENGINE_ID);
            }

            if (connection.connection().checkExists().forPath(RUNNERS_STATE + "/" + ENGINE_ID) == null) {
                connection.connection().create()
                        .creatingParentContainersIfNeeded()
                        .forPath(RUNNERS_STATE + "/" + ENGINE_ID);
            }
        } catch (Exception exception){
            throw new RuntimeException("Could not create Zookeeper paths in TaskRunner");
        }

        LOG.debug("Registered TaskRunner");
    }

    private synchronized int getRunningTasksCount() {
        return runningTasks.size();
    }

    private synchronized void addRunningTask(String id) {
        runningTasks.add(id);
        updateOwnState();
    }

    private synchronized void removeRunningTask(String id) {
        runningTasks.remove(id);
        updateOwnState();
    }

    private void seekAndCommit(TopicPartition partition, long offset) {
        consumer.seek(partition, offset);
        consumer.commitSync();
    }

    private class HandleRebalance implements ConsumerRebalanceListener {
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            LOG.debug("TaskRunner consumer partitions assigned " + partitions);
        }
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            consumer.commitSync();
            LOG.debug("TaskRunner consumer partitions revoked " + partitions);
        }
    }
}
