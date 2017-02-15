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

import ai.grakn.engine.backgroundtasks.TaskStateStorage;
import ai.grakn.engine.backgroundtasks.TaskState;
import ai.grakn.engine.util.ConfigProperties;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import static ai.grakn.engine.TaskStatus.SCHEDULED;
import static ai.grakn.engine.TaskStatus.STOPPED;
import static ai.grakn.engine.backgroundtasks.config.ConfigHelper.kafkaConsumer;
import static ai.grakn.engine.backgroundtasks.config.ConfigHelper.kafkaProducer;
import static ai.grakn.engine.backgroundtasks.config.KafkaTerms.NEW_TASKS_TOPIC;
import static ai.grakn.engine.backgroundtasks.config.KafkaTerms.SCHEDULERS_GROUP;
import static ai.grakn.engine.backgroundtasks.config.KafkaTerms.WORK_QUEUE_TOPIC;
import static ai.grakn.engine.util.ExceptionWrapper.noThrow;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;

/**
 * Handle execution of recurring tasks.
 * Monitor new tasks queue to add them to ScheduledExecutorService.
 * ScheduledExecutorService will be given a function to add the task in question to the work queue.
 *
 * @author Denis Lobanov
 */
public class Scheduler implements Runnable, AutoCloseable {
    private static final String STATUS_MESSAGE = "Topic [%s], partition [%s] received [%s] records, next offset is [%s]";

    private final static Logger LOG = LoggerFactory.getLogger(Scheduler.class);
    private final static int SCHEDULER_THREADS = ConfigProperties.getInstance().getAvailableThreads();
    private final AtomicBoolean OPENED = new AtomicBoolean(false);

    private final TaskStateStorage storage;

    private KafkaConsumer<String, String> consumer;
    private KafkaProducer<String, String> producer;
    private ScheduledExecutorService schedulingService;
    private CountDownLatch waitToClose;
    private volatile boolean running = false;

    public Scheduler(TaskStateStorage storage){
        this.storage = storage;

        if(OPENED.compareAndSet(false, true)) {
            // Kafka listener
            consumer = kafkaConsumer(SCHEDULERS_GROUP);
            consumer.subscribe(Collections.singletonList(NEW_TASKS_TOPIC), new HandleRebalance());

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
                ConsumerRecords<String, String> records = consumer.poll(1000);
                printConsumerStatus(records);

                long startTime = System.currentTimeMillis();
                for(ConsumerRecord<String, String> record:records) {

                    TaskState taskState = TaskState.deserialize(record.value());

                    // mark the task as created
                    storage.newState(taskState);

                    // schedule the task
                    scheduleTask(taskState);

                    //acknowledge that the record was read to the consumer
                    consumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset() + 1);
                }

                LOG.debug(format("Took [%s] ms to process [%s] records in scheduler", System.currentTimeMillis() - startTime, records.count()));
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
        long delay = Duration.between(Instant.now(), state.runAt()).toMillis();

        Runnable submit = () -> {
            markAsScheduled(state);
            sendToWorkQueue(state);
        };

        if(state.isRecurring()) {
            schedulingService.scheduleAtFixedRate(submit, delay, state.interval(), MILLISECONDS);
        }
        else {
            schedulingService.schedule(submit, delay, MILLISECONDS);
        }
    }

    /**
     * Mark the taskstorage of the given task as scheduled
     * @param state task to mark the state of
     */
    private void markAsScheduled(TaskState state) {
        LOG.debug("Marking " + state.getId() + " as scheduled");
        storage.updateState(state.status(SCHEDULED));
    }

    /**
     * Submit a task to the work queue
     * @param state task to be submitted
     */
    private void sendToWorkQueue(TaskState state) {
        LOG.debug("Sending to work queue " + state.getId());
        producer.send(new ProducerRecord<>(WORK_QUEUE_TOPIC, state.getId(), TaskState.serialize(state)), new KafkaLoggingCallback());
        producer.flush();
    }

    /**
     * Get all recurring tasks from the graph and schedule them
     */
    private void restartRecurringTasks() {
        Set<TaskState> tasks = storage.getTasks(null, null, null, 0, 0);
        tasks.stream()
                .filter(TaskState::isRecurring)
                .filter(p -> p.status() != STOPPED)
                .forEach(this::scheduleTask);
    }

    private void printConsumerStatus(ConsumerRecords<String, String> records){
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

    private static class HandleRebalance implements ConsumerRebalanceListener {
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            LOG.debug("Scheduler partitions assigned " + partitions);
        }
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            LOG.debug("Scheduler partitions revoked " + partitions);
        }
    }
}

