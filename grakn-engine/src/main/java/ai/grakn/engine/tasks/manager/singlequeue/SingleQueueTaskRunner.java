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
import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.tasks.ExternalOffsetStorage;
import ai.grakn.engine.tasks.TaskCheckpoint;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.engine.util.EngineID;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static ai.grakn.engine.TaskStatus.FAILED;
import static ai.grakn.engine.TaskStatus.RUNNING;
import static ai.grakn.engine.TaskStatus.STOPPED;
import static ai.grakn.engine.tasks.config.KafkaTerms.HIGH_PRIORITY_TASKS_TOPIC;
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
    private Instant timeTaskLastHandled;

    /**
     * Create a {@link SingleQueueTaskRunner} which retrieves tasks from the given {@param consumer} and uses the given
     * {@param storage} to store and retrieve information about tasks.
     *
     * @param engineID identifier of the engine this task runner is on
     * @param manager a place to control the lifecycle of tasks
     * @param offsetStorage a place to externally store kafka offsets
     */
    public SingleQueueTaskRunner(SingleQueueTaskManager manager, EngineID engineID, ExternalOffsetStorage offsetStorage, int timeUntilBackoff, Supplier<Consumer<TaskId, TaskState>> consumerSupplier){
        this.manager = manager;
        this.storage = manager.storage();
        this.consumer = consumerSupplier.get();
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

        timeTaskLastHandled = now();
        int backOff = INITIAL_BACKOFF;

        while (!wakeUp.get()) {
            try {
                // Reading from both the regular consumer and recurring consumer every time means that we will handle
                // recurring tasks regularly, even if there are lots of non-recurring tasks to process.
                readRecords(consumer);

                // Exponential back-off: sleep longer and longer when receiving the same tasks
                long timeSinceLastHandledTask = between(timeTaskLastHandled, now()).toMillis();
                if (timeSinceLastHandledTask >= MAX_TIME_SINCE_HANDLED_BEFORE_BACKOFF) {
                    LOG.debug("has been  " + timeSinceLastHandledTask + " ms since handled task, sleeping for " + backOff + "ms");
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
     * Read and handle some records from the given consumer
     */
    private void readRecords(Consumer<TaskId, TaskState> theConsumer) {
        // This TaskRunner should only ever receive one record from each consumer
        ConsumerRecords<TaskId, TaskState> records = theConsumer.poll(0);
        debugConsumerStatus(theConsumer, records);

        for (ConsumerRecord<TaskId, TaskState> record : records) {
            TaskState task = record.value();
            boolean handled = handleTask(task, record.topic());

            if (handled) {
                timeTaskLastHandled = now();
            }

            offsetStorage.saveOffset(theConsumer, new TopicPartition(record.topic(), record.partition()));

            LOG.trace("{} acknowledged", record.key().getValue());
        }
    }

    /**
     * Returns whether the task was succesfully handled, or was just re-submitted.
     */
    private boolean handleTask(TaskState taskFromkafka, String priority) {
        LOG.debug("{}\treceived", taskFromkafka);

        TaskState latestState = getLatestState(taskFromkafka);

        if (shouldStopTask(latestState)) {
            stopTask(latestState);
            return true;
        } else if(shouldDelayTask(latestState)){
            resubmitTask(latestState, priority);
            return false;
        } else {
            // Need updated state to reflect task state changes in the execute method
            TaskState updatedState = executeTask(latestState);

            if(taskShouldRecur(updatedState)){
                resubmitTask(updatedState, priority);
            }

            return true;
        }
    }

    /**
     * Execute a task.
     *
     * If a task is resuming, it should not be re-marked as running and should be started from it's checkpoint.
     * If a task is seen for the first time, it should be marked as running and started with it's original configuration.
     *
     * @param task the task to execute from the kafka queue
     */
    private TaskState executeTask(TaskState task){
        try {
            runningTaskId = task.getId();
            runningTask = task.taskClass().newInstance();

            boolean completed;

            //TODO pass a method to retrieve checkpoint from storage to task and remove "resume" method in interface
            if(taskShouldResume(task)){
                LOG.debug("{}\tresuming ", task);

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
            LOG.error(throwable.getMessage());
        } finally {
            runningTask = null;
            runningTaskId = null;

            // Update the schedule of the task if it should recur
            if(taskShouldRecur(task)) {
                task.schedule(task.schedule().incrementByInterval());
            }

            storage.updateState(task);

            LOG.debug("{}\tmarked as {}", task, task.status());
        }

        return task;
    }

    /**
     * Tasks are delayed by re-submitting them to the queue until it is time for them
     * to be executed
     * @param task Task to be delayed
     */
    private void resubmitTask(TaskState task, String priority){
        if(priority.equals(HIGH_PRIORITY_TASKS_TOPIC)){
            manager.addHighPriorityTask(task);
        } else {
            manager.addLowPriorityTask(task);
        }
        LOG.debug("{}\tresubmitted with {}", task, priority);
    }

    private void stopTask(TaskState task) {
        task.markStopped();
        putState(task);
        LOG.debug("{}\t marked as stopped", task);
    }

    /**
     * Tasks should be delayed when their schedule is set for after the current moment.
     * This applies to recurring tasks as well.
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
        return task.schedule().isRecurring() && !task.status().equals(FAILED) && !task.status().equals(STOPPED);
    }

    /**
     * Tasks should resume from the last checkpoint when their checkpoint is
     * non null and they are in the RUNNING state. This status should be taken from
     * the latest snapshot of task state in storage.
     *
     * Recurring tasks should
     * @param task Task that should be checked
     * @return If the given task can resume
     */
    private boolean taskShouldResume(TaskState task){
        return task.status() == RUNNING;
    }

    private boolean shouldStopTask(TaskState task) {
        return task.status() == STOPPED || manager.isTaskMarkedStopped(task.getId());
    }

    /**
     * Return the latest state of the task from storage. If the task
     * has not been saved in storage, return the state that was given.
     * @return Latest state of the given task
     */
    private TaskState getLatestState(TaskState task){
        if(storage.containsTask(task.getId())){
            return storage.getState(task.getId());
        }

        return task;
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
    private void debugConsumerStatus(Consumer<TaskId, TaskState> theConsumer, ConsumerRecords<TaskId, TaskState> records ){
        for (TopicPartition partition : theConsumer.assignment()) {
            LOG.trace("Partition {}{} has offset {} after receiving {} records",
                    partition.topic(), partition.partition(), theConsumer.position(partition), records.records(partition).size());
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
