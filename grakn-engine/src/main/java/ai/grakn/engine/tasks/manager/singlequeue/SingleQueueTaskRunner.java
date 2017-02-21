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

import ai.grakn.engine.TaskStatus;
import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskStateStorage;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.engine.TaskStatus.CREATED;
import static ai.grakn.engine.TaskStatus.FAILED;

/**
 * The {@link SingleQueueTaskRunner} is used by the {@link SingleQueueTaskManager} to execute tasks from a Kafka queue.
 *
 * @author aelred, alexandrorth
 */
public class SingleQueueTaskRunner implements Runnable, AutoCloseable {

    private final static Logger LOG = LoggerFactory.getLogger(SingleQueueTaskRunner.class);

    private final Consumer<String, String> consumer;
    private final TaskStateStorage storage;

    private final AtomicBoolean wakeUp = new AtomicBoolean(false);
    private final CountDownLatch countDownLatch = new CountDownLatch(1);

    /**
     * Create a {@link SingleQueueTaskRunner} which retrieves tasks from the given {@param consumer} and uses the given
     * {@param storage} to store and retrieve information about tasks.
     *
     * @param storage a place to store and retrieve information about tasks.
     * @param consumer a Kafka consumer from which to poll for tasks
     */
    public SingleQueueTaskRunner(TaskStateStorage storage, Consumer<String, String> consumer) {
        this.storage = storage;
        this.consumer = consumer;
    }

    /**
     * Poll Kafka for any new tasks. Will not return until {@link SingleQueueTaskRunner#close()} is called.
     * After receiving tasks, accept as many as possible, up to the maximum allowed number of tasks.
     * For each task, follow the workflow based on its type:
     *  - If not created or not in storage:
     *    - Record that this engine is running this task
     *      Record that this task is running
     *    - Send to thread pool for execution:
     *       - Use reflection to retrieve task
     *       - Start from checkpoint if necessary, or from beginning
     *       - Record that this engine is no longer running this task
     *         Mark as completed or failed
     *  - Acknowledge message in queue
     */
    @Override
    public void run() {
        LOG.debug("started");

        try {
            while (!wakeUp.get()) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    TaskState task = TaskState.deserialize(record.value());

                    LOG.debug("{}\thandling", task);

                    String taskId = task.getId();
                    boolean inStorage = storage.containsState(taskId);
                    // TODO: make this less shit
                    boolean storageMarkedCompletedOrFailed = inStorage && (storage.getState(taskId).status().equals(COMPLETED) || storage.getState(taskId).status().equals(FAILED));

                    if ((!task.status().equals(CREATED) || !inStorage) && !storageMarkedCompletedOrFailed) {
                        task.status(TaskStatus.RUNNING);

                        LOG.debug("{}\tmarked as running", task);

                        storage.updateState(task);

                        LOG.debug("{}\trecorded", task);

                        BackgroundTask backgroundTask;
                        try {
                            backgroundTask = task.taskClass().newInstance();
                        } catch (InstantiationException | IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }

                        try {
                            backgroundTask.start(null, task.configuration());
                            task.status(TaskStatus.COMPLETED);
                            LOG.debug("{}\tmarked as completed", task);
                        } catch (Exception e) {
                            task.status(FAILED);
                            LOG.debug("{}\tmarked as failed", task);
                        }

                        storage.updateState(task);

                        LOG.debug("{}\trecorded", task);
                    }

                    consumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset() + 1);
                    consumer.commitSync();

                    LOG.debug("{}\tacknowledged", task);
                }
            }
        } finally {
            countDownLatch.countDown();
            LOG.debug("stopped");
        }
    }

    /**
     * Close connection to Kafka and thread pool.
     *
     * Inform {@link SingleQueueTaskRunner#run()} method to stop and block until it returns.
     */
    @Override
    public void close() throws Exception {
        wakeUp.set(true);
        countDownLatch.await();
    }
}
