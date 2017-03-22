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

import ai.grakn.engine.TaskId;
import ai.grakn.engine.tasks.ExternalOffsetStorage;
import ai.grakn.engine.tasks.TaskSchedule;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.engine.tasks.manager.ZookeeperConnection;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.exception.EngineStorageException;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import static ai.grakn.engine.TaskStatus.CREATED;
import static ai.grakn.engine.TaskStatus.STOPPED;
import static ai.grakn.engine.tasks.config.ConfigHelper.kafkaConsumer;
import static ai.grakn.engine.tasks.config.ConfigHelper.kafkaProducer;
import static ai.grakn.engine.tasks.config.KafkaTerms.NEW_TASKS_TOPIC;
import static ai.grakn.engine.tasks.config.KafkaTerms.SCHEDULERS_GROUP;
import static ai.grakn.engine.tasks.config.KafkaTerms.WORK_QUEUE_TOPIC;
import static ai.grakn.engine.tasks.manager.ExternalStorageRebalancer.rebalanceListener;
import static ai.grakn.engine.util.ExceptionWrapper.noThrow;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;

/**
 * Handle execution of recurring tasks.
 * Monitor new tasks queue to add them to ScheduledExecutorService.
 * ScheduledExecutorService will be given a function to add the task in question to the work queue.
 *
 * @author Denis Lobanov, alexandraorth
 */
public class Scheduler implements Runnable, AutoCloseable {
    private static final String STATUS_MESSAGE = "Topic [%s], partition [%s] received [%s] records, next offset is [%s]";

    private final static Logger LOG = LoggerFactory.getLogger(Scheduler.class);
    private final static int SCHEDULER_THREADS = GraknEngineConfig.getInstance().getAvailableThreads();
    private final AtomicBoolean OPENED = new AtomicBoolean(false);

    private final TaskStateStorage storage;

    private Consumer<TaskId, TaskState> consumer;
    private Producer<TaskId, TaskState> producer;
    private ScheduledExecutorService schedulingService;
    private CountDownLatch waitToClose;
    private volatile boolean running = false;

    public Scheduler(TaskStateStorage storage, ZookeeperConnection connection){
        this.storage = storage;

        if(OPENED.compareAndSet(false, true)) {
            // Kafka listener
            consumer = kafkaConsumer(SCHEDULERS_GROUP);

            // Configure callback for a Kafka rebalance
            consumer.subscribe(singletonList(NEW_TASKS_TOPIC), rebalanceListener(consumer, new ExternalOffsetStorage(connection)));

            // Kafka writer
            producer = kafkaProducer();

            waitToClose = new CountDownLatch(1);

            ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                    .setNameFormat("scheduler-pool-%d").build();
            schedulingService = Executors.newScheduledThreadPool(SCHEDULER_THREADS, namedThreadFactory);

            LOG.debug("Scheduler started");
        }
        else {
            LOG.error("Scheduled already opened!");
        }
    }

    public void run() {
        running = true;

        // restart any recurring tasks in the graph
        restartRecurringTasks();

        try {
            while (running) {
                ConsumerRecords<TaskId, TaskState> records = consumer.poll(1000);
                printConsumerStatus(records);

                long startTime = System.currentTimeMillis();
                for(ConsumerRecord<TaskId, TaskState> record:records) {

                    // Get the task from kafka
                    TaskState taskState = record.value();


                    // Mark the task as created and schedule
                    try {
                        storage.newState(taskState);

                        // schedule the task
                        scheduleTask(taskState);
                    } catch (EngineStorageException e){
                        LOG.debug("Already processed " + taskState.getId());

                        // If that task is marked as created, re-schedule
                        if(storage.getState(taskState.getId()).status() == CREATED){
                            scheduleTask(taskState);
                        }
                    } finally {
                        //acknowledge that the record was read to the consumer
                        consumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset() + 1);
                    }
                }

                LOG.debug(format("Took [%s] ms to process [%s] records in scheduler",
                        System.currentTimeMillis() - startTime, records.count()));
            }
        }
        catch (WakeupException e) {
            LOG.debug("Shutting down scheduler consumer");
        } catch (Throwable t){
            LOG.error("Error in scheduler poll " + getFullStackTrace(t));
        } finally {
            noThrow(consumer::close, "Exception while closing consumer in Scheduler");
            noThrow(waitToClose::countDown, "Exception while counting down close latch in Scheduler");
        }
    }

    /**
     * Stop the main loop, causing run() to exit.
     *
     * noThrow() functions used here so that if an error occurs during execution of a
     * certain step, the subsequent stops continue to execute.
     */
    public void close() {
        if(OPENED.compareAndSet(true, false)) {
            running = false;
            noThrow(consumer::wakeup, "Could not wake up scheduler thread.");

            // Wait for thread calling run() to wakeup and close consumer.
            noThrow(waitToClose::await, "Error waiting for TaskRunner consumer to exit");

            noThrow(schedulingService::shutdownNow, "Could not shutdown scheduling service.");

            noThrow(producer::flush, "Could not flush Kafka producer in scheduler.");
            noThrow(producer::close, "Could not close Kafka producer in scheduler.");

            LOG.debug("Scheduler stopped.");
        }
        else {
            LOG.error("Scheduler open() must be called before close()!");
        }
    }

    /**
     * Schedule a task to be submitted to the work queue when it is supposed to be run
     * @param state state of the task
     */
    private void scheduleTask(TaskState state) {
        TaskSchedule schedule = state.schedule();
        long delay = Duration.between(Instant.now(), schedule.runAt()).toMillis();

        Runnable submit = () -> {
            markAsScheduled(state);
            sendToWorkQueue(state);
        };

        Optional<Duration> interval = schedule.interval();
        if(interval.isPresent()) {
            schedulingService.scheduleAtFixedRate(submit, delay, interval.get().toMillis(), MILLISECONDS);
        } else {
            schedulingService.schedule(submit, delay, MILLISECONDS);
        }
    }

    /**
     * Mark the taskstorage of the given task as scheduled
     * @param state task to mark the state of
     */
    private void markAsScheduled(TaskState state) {
        LOG.debug("Marking " + state.getId() + " as scheduled");
        storage.updateState(state.markScheduled());
    }

    /**
     * Submit a task to the work queue
     * @param state task to be submitted
     */
    private void sendToWorkQueue(TaskState state) {
        LOG.debug("Sending to work queue " + state.getId());
        producer.send(new ProducerRecord<>(WORK_QUEUE_TOPIC, state.getId(), state), new KafkaLoggingCallback());
        producer.flush();
    }

    /**
     * Get all recurring tasks from the graph and schedule them
     */
    private void restartRecurringTasks() {
        LOG.debug("Restarting recurring tasks");

        Set<TaskState> tasks = storage.getTasks(null, null, null, null, 0, 0);
        tasks.stream()
                .filter(state -> state.schedule().isRecurring())
                .filter(p -> p.status() != STOPPED)
                .forEach(this::scheduleTask);
    }

    private void printConsumerStatus(ConsumerRecords<TaskId, TaskState> records){
        consumer.assignment().stream()
                .map(p -> format(STATUS_MESSAGE, p.partition(), p.topic(), records.count(), consumer.position(p)))
                .forEach(LOG::debug);
    }

    private static class KafkaLoggingCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if(exception != null) {
                LOG.debug(getFullStackTrace(exception));
            }
        }
    }
}

