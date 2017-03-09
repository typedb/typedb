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
import ai.grakn.engine.tasks.TaskId;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.engine.util.EngineID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static ai.grakn.engine.TaskStatus.*;
import static ai.grakn.engine.util.ExceptionWrapper.noThrow;
import static java.time.Instant.now;

/**
 * The {@link SingleQueueTaskRunner} is used by the {@link SingleQueueTaskManager} to execute tasks from a Kafka queue.
 *
 * @author aelred, alexandrorth
 */
public class SingleQueueTaskRunner implements Runnable, AutoCloseable {

    private final static Logger LOG = LoggerFactory.getLogger(SingleQueueTaskRunner.class);

    private final Consumer<TaskId, TaskState> consumer;
    private final SingleQueueTaskManager manager;
    private final TaskStateStorage storage;

    private final AtomicBoolean wakeUp = new AtomicBoolean(false);
    private final CountDownLatch countDownLatch = new CountDownLatch(1);
    private final EngineID engineID;

    private TaskId runningTaskId = null;
    private BackgroundTask runningTask = null;

    /**
     * Create a {@link SingleQueueTaskRunner} which retrieves tasks from the given {@param consumer} and uses the given
     * {@param storage} to store and retrieve information about tasks.
     *
     * @param engineID identifier of the engine this task runner is on
     * @param manager a place to control the lifecycle of tasks
     */
    public SingleQueueTaskRunner(SingleQueueTaskManager manager, EngineID engineID){
        this.manager = manager;
        this.storage = manager.storage();
        this.consumer = manager.newConsumer();
        this.engineID = engineID;
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
     *       - Start from checkpoint if necessary, or from beginning (TODO)
     *       - Record that this engine is no longer running this task
     *         Mark as completed or failed
     *  - Acknowledge message in queue
     */
    @Override
    public void run() {
        LOG.debug("started");

        while (!wakeUp.get()) {
            try {
                ConsumerRecords<TaskId, TaskState> records = consumer.poll(1000);
                debugConsumerStatus(records);

                // This TskRunner should only ever receive one record
                for (ConsumerRecord<TaskId, TaskState> record : records) {
                    handleRecord(record.value());

                    consumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset() + 1);
                    consumer.commitSync();

                    LOG.trace("{} acknowledged", record.key().getValue());
                }
            } catch (Throwable throwable){
                LOG.error("error thrown", throwable);
            }
        }

        countDownLatch.countDown();
        LOG.debug("stopped");
    }

    /**
     * Close connection to Kafka and thread pool.
     *
     * Inform {@link SingleQueueTaskRunner#run()} method to stop and block until it returns.
     */
    @Override
    public void close() throws Exception {
        wakeUp.set(true);
        noThrow(countDownLatch::await, "Error waiting for the TaskRunner loop to finish");
        noThrow(consumer::close, "Error closing the task runner");
    }

    /**
     * Only
     * @param taskId
     * @return
     */
    public boolean stopTask(TaskId taskId) {
        return taskId.equals(runningTaskId) && runningTask.stop();
    }

    /**
     * Returns false if cannot handle record because the executor is full
     */
    private void handleRecord(TaskState task) {
        LOG.debug("{}\treceived", task);

        if(shouldDelayTask(task)){
            resubmitTask(task);
        }
        else if (shouldExecuteTask(task)) {
            executeTask(task);
        }

        if(taskShouldRecur(task)){
            // re-schedule
            resubmitTask(task);
        }
    }

    private void executeTask(TaskState task){
        // Mark as running
        task.markRunning(engineID);

        //TODO Make this a put within state storage
        if(storage.containsTask(task.getId())) {
            storage.updateState(task);
        } else {
            storage.newState(task);
        }

        LOG.debug("{}\tmarked as running", task);

        // Execute task
        try {
            runningTaskId = task.getId();
            runningTask = task.taskClass().newInstance();
            boolean completed = runningTask.start(null, task.configuration());
            if (completed) {
                task.markCompleted();
            } else {
                task.markStopped();
            }
            LOG.debug("{}\tmarked as completed", task);
        } catch (Throwable throwable) {
            task.markFailed(throwable);
            LOG.debug("{}\tmarked as failed", task);
        } finally {
            runningTask = null;
            runningTaskId = null;
            storage.updateState(task);
        }
    }

    /**
     * Tasks are delayed by re-submitting them to the queue until it is time for them
     * to be executed
     * @param task Task to be delayed
     */
    private void resubmitTask(TaskState task){
        manager.addTask(task);
    }

    private boolean shouldExecuteTask(TaskState task) {
        TaskId taskId = task.getId();

        if (task.status().equals(CREATED)) {
            // Only run created tasks if they are not being retried
            return !storage.containsTask(taskId);
        } else if(task.schedule().isRecurring()){
            TaskStatus status = storage.getState(taskId).status();
            return !status.equals(STOPPED) && !status.equals(FAILED);
        } else {
            // Only run retried tasks if they are not failed and (not completed and not recurring)
            // TODO: what if another task runner is running this task? (due to rebalance)
            TaskStatus status = storage.getState(taskId).status();
            return !status.equals(STOPPED) && !status.equals(COMPLETED) && !status.equals(FAILED);
        }
    }

    /**
     * Tasks should be delayed when their schedule is set for
     * @return If the provided task should be delayed.
     */
    private boolean shouldDelayTask(TaskState task){
        return !task.schedule().runAt().isBefore(now());
    }

    /**
     * Determine if task is recurring and should be re-run. Recurring tasks should not
     * be re-run when they are stopped or failed.
     * @return If the task should be run again
     */
    private boolean taskShouldRecur(TaskState task){
        return task.schedule().isRecurring() && !task.status().equals(FAILED)&& !task.status().equals(STOPPED);
    }

    /**
     * Log debug information about the given set of {@param records} polled from Kafka
     * @param records Polled-for records to return information about
     */
    private void debugConsumerStatus(ConsumerRecords<TaskId, TaskState> records ){
        for (TopicPartition partition : consumer.assignment()) {
            LOG.debug("Partition {}{} has offset {} after receiving {} records",
                    partition.topic(), partition.partition(), consumer.position(partition), records.records(partition).size());
        }
    }
}
