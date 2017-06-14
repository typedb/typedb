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
import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.tasks.ExternalOffsetStorage;
import ai.grakn.engine.tasks.TaskCheckpoint;
import ai.grakn.engine.tasks.TaskConfiguration;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.engine.tasks.connection.RedisConnection;
import ai.grakn.engine.util.EngineID;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import static com.codahale.metrics.MetricRegistry.name;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
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

    private final Consumer<TaskState, TaskConfiguration> consumer;
    private final SingleQueueTaskManager manager;
    private final TaskStateStorage storage;
    private final ExternalOffsetStorage offsetStorage;

    private final AtomicBoolean wakeUp = new AtomicBoolean(false);
    private final CountDownLatch countDownLatch = new CountDownLatch(1);
    private final EngineID engineID;
    private final GraknEngineConfig engineConfig;
    private final RedisConnection redis;
    private final Timer readRecordsTimer;
    private final Meter stopMeter;
    private final Meter resubmitMeter;
    private MetricRegistry metricRegistry;
    private final Timer executeTimer;

    private TaskId runningTaskId = null;
    private BackgroundTask runningTask = null;

    /**
     * Create a {@link SingleQueueTaskRunner} which retrieves tasks from the given {@param consumer} and uses the given
     * {@param storage} to store and retrieve information about tasks.
     *  @param manager a place to control the lifecycle of tasks
     * @param engineID identifier of the engine this task runner is on
     * @param offsetStorage a place to externally store kafka offsets
     * @param metricRegistry global metric registry
     */
    public SingleQueueTaskRunner(
            SingleQueueTaskManager manager, EngineID engineID, GraknEngineConfig config,
            RedisConnection redis,
            ExternalOffsetStorage offsetStorage,
            Consumer<TaskState, TaskConfiguration> consumer,
            MetricRegistry metricRegistry){
        this.manager = manager;
        this.storage = manager.storage();
        this.consumer = consumer;
        this.engineID = engineID;
        this.engineConfig = config;
        this.redis = redis;
        this.offsetStorage = offsetStorage;
        this.readRecordsTimer = metricRegistry
                .timer(name(SingleQueueTaskRunner.class, "read-records"));
        this.executeTimer = metricRegistry
                .timer(name(SingleQueueTaskRunner.class, "execute"));
        this.stopMeter = metricRegistry
                .meter(name(SingleQueueTaskRunner.class, "stop"));
        this.resubmitMeter = metricRegistry
                .meter(name(SingleQueueTaskRunner.class, "resubmit"));
        this.metricRegistry = metricRegistry;
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
                // Reading from both the regular consumer and recurring consumer every time means that we will handle
                // recurring tasks regularly, even if there are lots of non-recurring tasks to process.
                readRecords(consumer);
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
    private void readRecords(Consumer<TaskState, TaskConfiguration> theConsumer) {
        // This TaskRunner should only ever receive one record from each consumer
        Context context = readRecordsTimer.time();
        try{
            ConsumerRecords<TaskState, TaskConfiguration> records = theConsumer.poll(1000);
            for (ConsumerRecord<TaskState, TaskConfiguration> record : records) {
                TaskState task = record.key();
                TaskConfiguration configuration = record.value();
                handleTask(task, configuration);
                offsetStorage.saveOffset(theConsumer, new TopicPartition(record.topic(), record.partition()));
                LOG.trace("{} acknowledged", task.getId());
            }
        } finally {
            context.stop();
        }
    }

    /**
     * Returns whether the task was succesfully handled, or was just re-submitted.
     */
    private boolean handleTask(TaskState taskFromkafka, TaskConfiguration configuration) {
        LOG.debug("{}\treceived", taskFromkafka);

        TaskState latestState = getLatestState(taskFromkafka);

        if (shouldStopTask(latestState)) {
            stopTask(latestState);
            stopMeter.mark();
            return true;
        } else if(shouldDelayTask(latestState)){
            resubmitTask(latestState, configuration);
            resubmitMeter.mark();
            return false;
        } else {
            // Need updated state to reflect task state changes in the execute method
            TaskState updatedState = executeTask(latestState, configuration);

            if(taskShouldRecur(updatedState)){
                resubmitMeter.mark();
                resubmitTask(updatedState, configuration);
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
    private TaskState executeTask(TaskState task, TaskConfiguration configuration){
        Context context = executeTimer.time();
        try {
            runningTaskId = task.getId();
            runningTask = task.taskClass().newInstance();
            runningTask.initialize(saveCheckpoint(task), configuration, manager, engineConfig, redis,
                    metricRegistry);
            boolean completed;

            //TODO pass a method to retrieve checkpoint from storage to task and remove "resume" method in interface
            if(taskShouldResume(task)){
                LOG.debug("{}\tresuming ", task);

                //Mark as running
                task.markRunning(engineID);

                putState(task);

                completed = runningTask.resume(task.checkpoint());
            } else {
                //Mark as running
                task.markRunning(engineID);

                putState(task);

                LOG.debug("{}\tmarked as running", task);

                completed = runningTask.start();
            }

            if (completed) {
                task.markCompleted();
            } else {
                task.markStopped();
            }
        } catch (Throwable throwable) {
            task.markFailed(throwable);
            LOG.error("{}\tfailed with {}", task.getId(), throwable.getMessage());
        } finally {
            runningTask = null;
            runningTaskId = null;

            // Update the schedule of the task if it should recur
            if(taskShouldRecur(task)) {
                task.schedule(task.schedule().incrementByInterval());
            }

            storage.updateState(task);

            LOG.debug("{}\tmarked as {}", task, task.status());
            context.stop();
        }

        return task;
    }

    /**
     * Tasks are delayed by re-submitting them to the queue until it is time for them
     * to be executed
     * @param task Task to be delayed
     */
    private void resubmitTask(TaskState task, TaskConfiguration configuration){
        manager.addTask(task, configuration);
        LOG.debug("{}\tresubmitted with {}", task, task.priority().queue());
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

    /**
     * Checks whether the task should be stopped.
     *
     * This is decided by inspecting the current status of the task, or by contacting the
     * {@link SingleQueueTaskManager} to see whether the task has been requested to be stopped.
     */
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
     * Persists a Background Task's checkpoint to ZK and graph.
     * @param taskState task to update in storage
     * @return A Consumer<String> function that can be called by the background task on demand to save its checkpoint.
     */
    private java.util.function.Consumer<TaskCheckpoint> saveCheckpoint(TaskState taskState) {
        return checkpoint -> storage.updateState(taskState.checkpoint(checkpoint));
    }
}
