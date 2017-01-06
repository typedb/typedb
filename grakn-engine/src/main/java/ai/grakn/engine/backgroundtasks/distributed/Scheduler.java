/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Ltd
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

import ai.grakn.engine.backgroundtasks.TaskState;
import ai.grakn.engine.util.ConfigProperties;
import javafx.util.Pair;
import ai.grakn.engine.backgroundtasks.taskstorage.GraknStateStorage;
import ai.grakn.engine.backgroundtasks.taskstorage.SynchronizedStateStorage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.json.JSONObject;

import java.util.Collections;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static ai.grakn.engine.backgroundtasks.TaskStatus.STOPPED;
import static ai.grakn.engine.backgroundtasks.config.ConfigHelper.kafkaConsumer;
import static ai.grakn.engine.backgroundtasks.config.ConfigHelper.kafkaProducer;
import static ai.grakn.engine.backgroundtasks.config.KafkaTerms.NEW_TASKS_TOPIC;
import static ai.grakn.engine.backgroundtasks.config.KafkaTerms.SCHEDULERS_GROUP;
import static ai.grakn.engine.backgroundtasks.config.KafkaTerms.WORK_QUEUE_TOPIC;
import static ai.grakn.engine.util.ConfigProperties.SCHEDULER_POLLING_FREQ;
import static ai.grakn.engine.util.ExceptionWrapper.noThrow;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static ai.grakn.engine.backgroundtasks.TaskStatus.SCHEDULED;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;

/**
 * Handle execution of recurring tasks.
 * Monitor new tasks queue to add them to ScheduledExecutorService.
 * ScheduledExecutorService will be given a function to add the task in question to the work queue.
 */
public class Scheduler implements Runnable, AutoCloseable {
    private final static ConfigProperties properties = ConfigProperties.getInstance();
    private final KafkaLogger LOG = KafkaLogger.getInstance();
    private final AtomicBoolean OPENED = new AtomicBoolean(false);

    private GraknStateStorage stateStorage;
    private SynchronizedStateStorage zkStorage;
    private KafkaConsumer<String, String> consumer;
    private KafkaProducer<String, String> producer;
    private ScheduledExecutorService schedulingService;
    private CountDownLatch waitToClose;
    private boolean initialised = false;
    private volatile boolean running = false;

    public void run() {
        running = true;

        // restart any recurring tasks in the graph
        restartRecurringTasks();

        try {
            while (running) {
                printInitialization();
                LOG.debug("Scheduler polling, size of new tasks " + consumer.endOffsets(consumer.partitionsFor(NEW_TASKS_TOPIC).stream().map(i -> new TopicPartition(NEW_TASKS_TOPIC, i.partition())).collect(toSet())));

                ConsumerRecords<String, String> records = consumer.poll(properties.getPropertyAsInt(SCHEDULER_POLLING_FREQ));

                for(ConsumerRecord<String, String> record:records) {
                    LOG.debug(String.format("Scheduler received topic = %s, partition = %s, offset = %s, taskid = %s, value = %s\n",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value()));

                    scheduleTask(record.key(), record.value());

                    //acknowledge that the record was read to the consumer
                    LOG.debug("Scheduler acknowledging " + record.key() + " OFFSET " + (record.offset()+1) + " topic " + record.topic());
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

    public Scheduler open() throws Exception {
        if(OPENED.compareAndSet(false, true)) {
            // Init task storage
            stateStorage = new GraknStateStorage();

            // Kafka listener
            consumer = kafkaConsumer(SCHEDULERS_GROUP);
            consumer.subscribe(Collections.singletonList(NEW_TASKS_TOPIC), new RebalanceListener(consumer));

            // Kafka writer
            producer = kafkaProducer();

            // ZooKeeper client
            zkStorage = SynchronizedStateStorage.getInstance();

            waitToClose = new CountDownLatch(1);

            schedulingService = Executors.newScheduledThreadPool(1);

            LOG.debug("Scheduler started");
        }
        else {
            LOG.error("Scheduled already opened!");
        }

        return this;
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

            noThrow(schedulingService::shutdown, "Could not shutdown scheduling service.");

            noThrow(producer::flush, "Could not flush Kafka producer in scheduler.");
            noThrow(producer::close, "Could not close Kafka producer in scheduler.");

            stateStorage = null;

            // Closed by ClusterManager
            zkStorage = null;

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
        TaskState state = stateStorage.getState(id);
        scheduleTask(id, configuration, state);
    }

    /**
     * Schedule a task to be submitted to the work queue when it is supposed to be run
     * @param id id of the task to be scheduled
     * @param configuration configuration of task to be scheduled, will be copied to WORK_QUEUE_TOPIC
     * @param state state of the task
     */
    private void scheduleTask(String id,  String configuration, TaskState state) {
        long delay = state.runAt().getTime() - new Date().getTime();

        markAsScheduled(id);
        if(state.isRecurring()) {
            LOG.debug("Scheduling recurring " + id);

            Runnable submit = () -> {
                markAsScheduled(id);
                sendToWorkQueue(id, configuration);
            };
            schedulingService.scheduleAtFixedRate(submit, delay, state.interval(), MILLISECONDS);
        }
        else {
            LOG.debug("Scheduling once " + id+" @ "+delay);
            Runnable submit = () -> sendToWorkQueue(id, configuration);
            schedulingService.schedule(submit, delay, MILLISECONDS);
        }
    }

    /**
     * Mark the taskstorage of the given task as scheduled
     * @param id task to mark the taskstorage of
     */
    private void markAsScheduled(String id) {
        LOG.debug("Marking " + id + " as scheduled");
        zkStorage.updateState(id, SCHEDULED, null, null);
        stateStorage.updateState(id, SCHEDULED, this.getClass().getName(), null, null, null, null);
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
        Set<Pair<String, TaskState>> tasks = stateStorage.getTasks(null, null, null, 0, 0, true);
        tasks.stream()
                .filter(p -> p.getValue().status() != STOPPED)
                .forEach(p -> {
                	// Not sure what is the right format for "no configuration", but somehow the configuration
                	// here for a postprocessing task is "null": if we say that the configuration of a task
                	// is a JSONObject, then an empty configuration ought to be {}
                	String config = p.getValue().configuration() == null ? "{}" : p.getValue().configuration().toString();
                	scheduleTask(p.getKey(), config, p.getValue());	
                });
        LOG.debug("Scheduler restarted " + tasks.size() + " recurring tasks");
    }

    private class KafkaLoggingCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if(exception != null) {
                LOG.debug(getFullStackTrace(exception));
            }
        }
    }

    private void printInitialization() {
        if(!initialised) {
            initialised = true;
            LOG.info("Scheduler initialised");
        }
    }
}

