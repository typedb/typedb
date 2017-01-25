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
import ai.grakn.engine.backgroundtasks.TaskManager;
import ai.grakn.engine.backgroundtasks.TaskState;
import ai.grakn.engine.backgroundtasks.TaskStatus;
import ai.grakn.engine.backgroundtasks.config.ConfigHelper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static ai.grakn.engine.backgroundtasks.TaskStatus.COMPLETED;
import static ai.grakn.engine.backgroundtasks.TaskStatus.FAILED;
import static ai.grakn.engine.backgroundtasks.TaskStatus.STOPPED;
import static ai.grakn.engine.backgroundtasks.config.KafkaTerms.NEW_TASKS_TOPIC;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;

/**
 * Class to manage tasks distributed using Kafka.
 */
public class DistributedTaskManager implements TaskManager {
    private final Logger LOG = LoggerFactory.getLogger(DistributedTaskManager.class);
    private final AtomicBoolean OPENED = new AtomicBoolean(false);

    private final KafkaProducer<String, String> producer;
    private final TaskStateStorage stateStorage;

    public DistributedTaskManager(TaskStateStorage stateStorage) {
        this.stateStorage = stateStorage;

        if(OPENED.compareAndSet(false, true)) {
            this.producer = ConfigHelper.kafkaProducer();
        }
        else {
            throw new RuntimeException("DistributedTaskManager open() called multiple times!");
        }
    }

    @Override
    public void close() {
        if(OPENED.compareAndSet(true, false)) {
            producer.close();
        }
        else {
            throw new RuntimeException("DistributedTaskManager close() called before open()!");
        }
    }

    @Override
    public String scheduleTask(BackgroundTask task, String createdBy, Instant runAt, long period, JSONObject configuration) {
        Boolean recurring = period > 0;

        TaskState taskState = new TaskState(task.getClass().getName())
                .creator(createdBy)
                .runAt(runAt)
                .isRecurring(recurring)
                .interval(period)
                .configuration(configuration);

        stateStorage.newState(taskState);
        try {
            producer.send(new ProducerRecord<>(NEW_TASKS_TOPIC, taskState.getId(), configuration.toString()));
            producer.flush();
        }
        catch (Exception e) {
            stateStorage.updateState(taskState.status(FAILED));
            LOG.error("Could not send task to Kafka " + getFullStackTrace(e));
        }

        return taskState.getId();
    }

    @Override
    public TaskManager stopTask(String id, String requesterName) {
        throw new UnsupportedOperationException(this.getClass().getName()+" currently doesnt support stopping tasks");
    }

    @Override
    public TaskStateStorage storage() {
        return stateStorage;
    }

    @Override
    public CompletableFuture completableFuture(String taskId) {
        return CompletableFuture.runAsync(() -> {

            while (true) {
                TaskStatus status = stateStorage.getState(taskId).status();
                if (status == COMPLETED || status == FAILED || status ==  STOPPED) {
                    break;
                }

                try {
                    Thread.sleep(5000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public TaskStatus getState(String taskID){
        return stateStorage.getState(taskID).status();
    }
}
