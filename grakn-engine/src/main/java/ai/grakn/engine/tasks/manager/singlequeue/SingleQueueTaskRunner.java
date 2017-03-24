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
import ai.grakn.engine.TaskStatus;
import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.tasks.ExternalOffsetStorage;
import ai.grakn.engine.tasks.TaskCheckpoint;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.engine.util.EngineID;
import java.time.Instant;
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
import static ai.grakn.engine.TaskStatus.RUNNING;
import static ai.grakn.engine.TaskStatus.STOPPED;
import static ai.grakn.engine.util.ExceptionWrapper.noThrow;
import static java.time.Duration.between;
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
    private final ExternalOffsetStorage offsetStorage;

    private final AtomicBoolean wakeUp = new AtomicBoolean(false);
    private final CountDownLatch countDownLatch = new CountDownLatch(1);
    private final EngineID engineID;

    private TaskId runningTaskId = null;
    private BackgroundTask runningTask = null;

    private static final int INITIAL_BACKOFF = 1_000;
    private static final int MAX_BACKOFF = 60_000;
    private final int MAX_TIME_SINCE_HANDLED_BEFORE_BACKOFF;

    /**
     * Create a {@link SingleQueueTaskRunner} which retrieves tasks from the given {@param consumer} and uses the given
     * {@param storage} to store and retrieve information about tasks.
     *
     * @param engineID identifier of the engine this task runner is on
     * @param manager a place to control the lifecycle of tasks
     * @param offsetStorage a place to externally store kafka offsets
     */
    public SingleQueueTaskRunner(SingleQueueTaskManager manager, EngineID engineID, ExternalOffsetStorage offsetStorage, int timeUntilBackoff){
        this.manager = manager;
        this.storage = manager.storage();
        this.consumer = manager.newConsumer();
        this.engineID = engineID;
        this.offsetStorage = offsetStorage;
        this.MAX_TIME_SINCE_HANDLED_BEFORE_BACKOFF = timeUntilBackoff;
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

        Instant timeTaskLastHandled = now();
        int backOff = INITIAL_BACKOFF;

        while (!wakeUp.get()) {
            try {
                ConsumerRecords<TaskId, TaskState> records = consumer.poll(1000);
                debugConsumerStatus(records);

                // This TskRunner should only ever receive one record
                for (ConsumerRecord<TaskId, TaskState> record : records) {
                    TaskState task = record.value();
                    boolean handled = handleTask(task);

                    if (handled) {
                        timeTaskLastHandled = now();
                    }

                    offsetStorage.saveOffset(consumer, new TopicPartition(record.topic(), record.partition()));

                    LOG.trace("{} acknowledged", record.key().getValue());
                }

                // Exponential back-off: sleep longer and longer when receiving the same tasks
                long timeSinceLastHandledTask = between(timeTaskLastHandled, now()).toMillis();
                if (timeSinceLastHandledTask >= MAX_TIME_SINCE_HANDLED_BEFORE_BACKOFF) {
                    LOG.debug("has been  " + timeSinceLastHandledTask + " ms since handeled task, sleeping for " + backOff + "ms");
                    Thread.sleep(backOff);
                    backOff *= 2;
                    if (backOff > MAX_BACKOFF) backOff = MAX_BACKOFF;
                } else {
                    backOff = INITIAL_BACKOFF;
                }

            } catch (Throwable throwable){
                LOG.error("error thrown", throwable);
                assert false; // This should be unreachable, but in production we still handle it for robustness
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
     * Stop the task if it is executing on this machine
     * @param taskId Identifier of the task to stop
     * @return True if the task is stopped
     */
    public boolean stopTask(TaskId taskId) {
        return taskId.equals(runningTaskId) && runningTask.stop();
    }

    /**
     * Returns whether the task was succesfully handled, or was just re-submitted.
     */
    private boolean handleTask(TaskState task) {
        LOG.debug("{}\treceived", task);

        if (shouldStopTask(task)) {
            stopTask(task);
            return true;
        } else if(shouldDelayTask(task)){
            resubmitTask(task);
            return false;
        } else if (shouldExecuteTask(task)) {
            executeTask(task);

            if(taskShouldRecur(task)){
                // re-schedule
                task.schedule(task.schedule().incrementByInterval());
                resubmitTask(task);
            }

            return true;
        } else {
            return true;
        }
    }

    /**
     * Execute a task.
     *
     * If a task is resuming, it should not be re-marked as running and should be started from it's checkpoint.
     * If a task is seen for the first time, it should be marked as running and started with it's original configuration.
     *
     * @param task the task to execute
     */
    private void executeTask(TaskState task){
        // Execute task
        try {
            runningTaskId = task.getId();
            runningTask = task.taskClass().newInstance();

            boolean completed;
            if(taskShouldResume(task)){
                completed = runningTask.resume(saveCheckpoint(task), task.checkpoint());
            } else {
                //Mark as running
                task.markRunning(engineID);

                putState(task);

                LOG.debug("{}\tmarked as running", task);

                completed = runningTask.start(saveCheckpoint(task), task.configuration());
            }

            if (completed) {
                task.markCompleted();
            } else {
                task.markStopped();
            }
        } catch (Throwable throwable) {
            task.markFailed(throwable);
        } finally {
            runningTask = null;
            runningTaskId = null;
            storage.updateState(task);
            LOG.debug("{}\tmarked as {}", task, task.status());
        }
    }

    /**
     * Tasks are delayed by re-submitting them to the queue until it is time for them
     * to be executed
     * @param task Task to be delayed
     */
    private void resubmitTask(TaskState task){
        manager.addTask(task);
        LOG.debug("{}\tresubmitted", task);
    }

    private void stopTask(TaskState task) {
        task.markStopped();
        putState(task);
        LOG.debug("{}\t marked as stopped", task);
    }

    private boolean shouldExecuteTask(TaskState task) {
        TaskId taskId = task.getId();

        if (task.status().equals(CREATED)) {
            // Only run created tasks if they are not being retried
            return !storage.containsTask(taskId);
        } else {
            // Only run retried tasks if they are not failed and if not completed or recurring)
            // TODO: what if another task runner is running this task? (due to rebalance)
            TaskStatus status = storage.getState(taskId).status();
            return !status.equals(STOPPED) && !status.equals(FAILED) &&
                    (!status.equals(COMPLETED) || task.schedule().isRecurring());
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
     * Tasks should resume from the last checkpoint when their checkpoint is
     * non null and they are in the RUNNING state.
     *
     * Recurring tasks should
     * @param task Task that should be checked
     * @return If the given task can resume
     */
    private boolean taskShouldResume(TaskState task){
        return task.checkpoint() != null && task.status().equals(RUNNING);
    }

    private boolean shouldStopTask(TaskState task) {
        return manager.isTaskMarkedStopped(task.getId());
    }

    private void putState(TaskState taskState) {
        //TODO Make this a put within state storage
        if(storage.containsTask(taskState.getId())) {
            storage.updateState(taskState);
        } else {
            storage.newState(taskState);
        }
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

    /**
     * Persists a Background Task's checkpoint to ZK and graph.
     * @param taskState task to update in storage
     * @return A Consumer<String> function that can be called by the background task on demand to save its checkpoint.
     */
    private java.util.function.Consumer<TaskCheckpoint> saveCheckpoint(TaskState taskState) {
        return checkpoint -> storage.updateState(taskState.checkpoint(checkpoint));
    }
}
