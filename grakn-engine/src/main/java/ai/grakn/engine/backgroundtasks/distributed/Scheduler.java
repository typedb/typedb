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
import java.util.concurrent.atomic.AtomicBoolean;

import static ai.grakn.engine.backgroundtasks.TaskStatus.SCHEDULED;
import static ai.grakn.engine.backgroundtasks.TaskStatus.STOPPED;
import static ai.grakn.engine.backgroundtasks.config.ConfigHelper.kafkaConsumer;
import static ai.grakn.engine.backgroundtasks.config.ConfigHelper.kafkaProducer;
import static ai.grakn.engine.backgroundtasks.config.KafkaTerms.NEW_TASKS_TOPIC;
import static ai.grakn.engine.backgroundtasks.config.KafkaTerms.SCHEDULERS_GROUP;
import static ai.grakn.engine.backgroundtasks.config.KafkaTerms.WORK_QUEUE_TOPIC;
import static ai.grakn.engine.util.ConfigProperties.SCHEDULER_POLLING_FREQ;
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

    private final static ConfigProperties properties = ConfigProperties.getInstance();
    private final static Logger LOG = LoggerFactory.getLogger(Scheduler.class);
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

            schedulingService = Executors.newScheduledThreadPool(1);

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

                for(ConsumerRecord<String, String> record:records) {
                    scheduleTask(record.key(), record.value());

                    //acknowledge that the record was read to the consumer
                    consumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset() + 1);
                }
            }
        }
        catch (WakeupException e) {
            // do nothing to shutdown
            LOG.debug("Shutting down scheduler consumer");
        }
        finally {
            consumer.commitSync();
            consumer.close();
            waitToClose.countDown();
        }
    }

    public void close() {
        if(OPENED.compareAndSet(true, false)) {
            running = false;
            noThrow(consumer::wakeup, "Could not wake up scheduler thread.");

            // Wait for thread calling run() to wakeup and close consumer.
            try {
                waitToClose.await(5*properties.getPropertyAsLong(SCHEDULER_POLLING_FREQ), MILLISECONDS);
            } catch (Throwable t) {
                LOG.error("Exception whilst waiting for scheduler run() thread to finish - " + getFullStackTrace(t));
            }

            noThrow(schedulingService::shutdownNow, "Could not shutdown scheduling service.");

            noThrow(producer::flush, "Could not flush Kafka producer in scheduler.");
            noThrow(producer::close, "Could not close Kafka producer in scheduler.");

            noThrow(() -> LOG.debug("Scheduler stopped."), "Kafka logging error.");
        }
        else {
            LOG.error("Scheduler open() must be called before close()!");
        }
    }

    /**
     * Schedule a task to be submitted to the work queue when it is supposed to be run
     * @param id id of the task to be scheduled
     * @param configuration configuration of task to be scheduled, will be copied to WORK_QUEUE_TOPIC
     */
    private void scheduleTask(String id, String configuration) {
        TaskState state = storage.getState(id);
        scheduleTask(id, configuration, state);
    }

    /**
     * Schedule a task to be submitted to the work queue when it is supposed to be run
     * @param id id of the task to be scheduled
     * @param configuration configuration of task to be scheduled, will be copied to WORK_QUEUE_TOPIC
     * @param state state of the task
     */
    private void scheduleTask(String id,  String configuration, TaskState state) {
        long delay = Duration.between(Instant.now(), state.runAt()).toMillis();

        markAsScheduled(state);
        if(state.isRecurring()) {
            Runnable submit = () -> {
                markAsScheduled(state);
                sendToWorkQueue(id, configuration);
            };
            schedulingService.scheduleAtFixedRate(submit, delay, state.interval(), MILLISECONDS);
        }
        else {
            Runnable submit = () -> sendToWorkQueue(id, configuration);
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
     * @param taskId id of the task to be submitted
     * @param configuration task to be submitted
     */
    private void sendToWorkQueue(String taskId, String configuration) {
        LOG.debug("Sending to work queue " + taskId);
        producer.send(new ProducerRecord<>(WORK_QUEUE_TOPIC, taskId, configuration), new KafkaLoggingCallback());
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
                .forEach(p -> {
                    // Not sure what is the right format for "no configuration", but somehow the configuration
                    // here for a postprocessing task is "null": if we say that the configuration of a task
                    // is a JSONObject, then an empty configuration ought to be {}
                    String config = p.configuration() == null ? "{}" : p.configuration().toString();
                    scheduleTask(p.getId(), config, p);
                });
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

    private class HandleRebalance implements ConsumerRebalanceListener {
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            LOG.debug("Scheduler partitions assigned " + partitions);
        }
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            consumer.commitSync();
            LOG.debug("Scheduler partitions revoked " + partitions);
        }
    }
}

