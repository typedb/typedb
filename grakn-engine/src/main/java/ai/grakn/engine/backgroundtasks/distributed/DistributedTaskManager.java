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
import ai.grakn.engine.util.EngineID;
import ai.grakn.engine.util.ExceptionWrapper;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static ai.grakn.engine.backgroundtasks.TaskStatus.*;
import static ai.grakn.engine.backgroundtasks.config.KafkaTerms.NEW_TASKS_TOPIC;
import static ai.grakn.engine.util.ExceptionWrapper.noThrow;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;

/**
 * Class to manage tasks distributed using Kafka.
 */
public class DistributedTaskManager implements TaskManager{
	private final Logger LOG = LoggerFactory.getLogger(DistributedTaskManager.class);
	private final AtomicBoolean OPENED = new AtomicBoolean(false);
    private static DistributedTaskManager instance = null;

    private KafkaProducer producer;
    private StateStorage stateStorage;
    private SynchronizedStateStorage zkStorage;

    public static synchronized DistributedTaskManager getInstance() {
        if(instance == null)
            instance = new DistributedTaskManager();
        return instance;
    }

    public TaskManager open() {
        if(OPENED.compareAndSet(false, true)) {
            try {
                producer = ConfigHelper.kafkaProducer();
                stateStorage = new GraknStateStorage();
                zkStorage = SynchronizedStateStorage.getInstance();
            }
            catch (Exception e) {
                e.printStackTrace(System.err);
                LOG.error("While trying to start the DistributedTaskManager", e);
                throw new RuntimeException("Could not start task manager : "+e);
            }
        }
        else {
            LOG.error("DistributedTaskManager open() called multiple times!");
        }

        return this;
    }

    @Override
    public void close() {
        if(OPENED.compareAndSet(true, false)) {
            noThrow(producer::close, "Could not close Kafka Producer.");
            stateStorage = null;
            zkStorage = null;
        }
        else {
            LOG.error("DistributedTaskManager close() called before open()!");
        }
    }

    @Override
    public String scheduleTask(BackgroundTask task, String createdBy, Date runAt, long period, JSONObject configuration) {
        Boolean recurring = period > 0;

        String id = stateStorage.newState(task.getClass().getName(), createdBy, runAt, recurring, period, configuration);
        try {
            zkStorage.newState(id, CREATED, null, null);

            producer.send(new ProducerRecord<>(NEW_TASKS_TOPIC, id, configuration.toString()));
            producer.flush();
        }
        catch (Exception e) {
            LOG.error("Could not write to ZooKeeper! - "+ getFullStackTrace(e));
            stateStorage.updateState(id, FAILED, this.getClass().getName(), EngineID.getInstance().id(), e, null, null);
            id = null;
        }

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
