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

import ai.grakn.engine.backgroundtasks.BackgroundTask;
import ai.grakn.engine.backgroundtasks.StateStorage;
import ai.grakn.engine.backgroundtasks.TaskManager;
import ai.grakn.engine.backgroundtasks.TaskStatus;
import ai.grakn.engine.backgroundtasks.config.ConfigHelper;
import ai.grakn.engine.backgroundtasks.taskstorage.GraknStateStorage;
import ai.grakn.engine.backgroundtasks.taskstorage.SynchronizedStateStorage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

import java.util.Date;
import java.util.concurrent.CompletableFuture;

import static ai.grakn.engine.backgroundtasks.TaskStatus.*;
import static ai.grakn.engine.backgroundtasks.config.KafkaTerms.NEW_TASKS_TOPIC;

/**
 * Class to manage tasks distributed using Kafka.
 */
public class DistributedTaskManager implements TaskManager, AutoCloseable {
    private KafkaProducer producer;

    private StateStorage stateStorage;
    private SynchronizedStateStorage zkStorage;

    /**
     * Instantiate connection with Zookeeper. Create Kafka producer. Start TaskRunner. Attempt to start Scheduler.
     */
    public DistributedTaskManager() {
        try {
            producer = ConfigHelper.kafkaProducer();
            stateStorage = new GraknStateStorage();
            zkStorage = SynchronizedStateStorage.getInstance();
        }
        catch (Exception e) {
            throw new RuntimeException("Could not start task manager : "+e);
        }
    }

    @Override
    public void close() {
        producer.close();
    }

    @Override
    public String scheduleTask(BackgroundTask task, String createdBy, Date runAt, long period, JSONObject configuration) {
        Boolean recurring = period > 0;

        String id = stateStorage.newState(task.getClass().getName(), createdBy, runAt, recurring, period, configuration);
        zkStorage.newState(id, CREATED, null, null);

        producer.send(new ProducerRecord<>(NEW_TASKS_TOPIC, id, configuration.toString()));
        producer.flush();

        return id;
    }

    @Override
    public TaskManager stopTask(String id, String requesterName) {
        throw new UnsupportedOperationException(this.getClass().getName()+" currently doesnt support stopping tasks");
    }

    @Override
    public StateStorage storage() {
        return stateStorage;
    }

    @Override
    public CompletableFuture completableFuture(String taskId) {
        return CompletableFuture.runAsync(() -> {

            while (true) {
                TaskStatus status = zkStorage.getState(taskId).status();
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
        return zkStorage.getState(taskID).status();
    }
}
