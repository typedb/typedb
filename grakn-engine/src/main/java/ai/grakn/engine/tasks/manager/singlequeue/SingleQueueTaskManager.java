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

import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.tasks.TaskManager;
import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.engine.tasks.config.ConfigHelper;
import ai.grakn.engine.tasks.manager.ZookeeperConnection;
import ai.grakn.engine.tasks.storage.TaskStateZookeeperStore;
import mjson.Json;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

import static ai.grakn.engine.tasks.config.ConfigHelper.client;
import static ai.grakn.engine.util.ExceptionWrapper.noThrow;

/**
 * {@link TaskManager} implementation that operates using a single Kafka queue and controls the
 * lifecycle {@link SingleQueueTaskManager}
 *
 * @author aelred, alexandrorth
 */
public class SingleQueueTaskManager implements TaskManager {

    private final static Logger LOG = LoggerFactory.getLogger(SingleQueueTaskManager.class);

    private final KafkaProducer<String, String> producer;
    private final ZookeeperConnection zookeeper;
    private final TaskStateStorage storage;

    private SingleQueueTaskRunner taskRunner;
    private Thread taskRunnerThread;

    /**
     * Create a {@link SingleQueueTaskManager}
     *
     * The SingleQueueTaskManager implementation must:
     *  + Instantiate a connection to zookeeper
     *  + Configure and instance of TaskStateStorage
     *  + Create and run an instance of SingleQueueTaskRunner
     */
    public SingleQueueTaskManager(){
        this.zookeeper = new ZookeeperConnection(client());
        this.storage = new TaskStateZookeeperStore(zookeeper);

        this.producer = ConfigHelper.kafkaProducer();
    }

    /**
     * Close the {@link SingleQueueTaskRunner} and . Any errors that occur should not prevent the
     * subsequent ones from executing.
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        LOG.debug("Closing TaskManager");

        // close kafka producer
        noThrow(producer::close, "Error shutting down producer in TaskManager");

        // close task runner
        noThrow(taskRunner::close, "Error shutting down TaskRunner");
        noThrow(taskRunnerThread::join, "Error waiting for TaskRunner to close");

        // stop zookeeper connection
        noThrow(zookeeper::close, "Error waiting for zookeeper connection to close");

        LOG.debug("TaskManager closed");
    }

    /**
     * Create an instance of a task based on the given parameters and submit it a Kafka queue.
     *
     * @param taskClass The class implementing the BackgroundTask interface
     * @param createdBy Name of the class that created the task
     * @param runAt Instant when task should run.
     * @param period A non-zero value indicates that this should be a recurring task and period indicates the delay between
     *               subsequent runs of the task after successful execution.
     * @param configuration A JSONObject instance containing configuration and optionally data for the task. This is an
     *                      optional parameter and may be set to null to not pass any configuration (task.start() will
     *                      get an initialised but empty JSONObject).
     * @return String identifier of the created task
     */
    @Override
    public String createTask(Class<? extends BackgroundTask> taskClass, String createdBy, Instant runAt, long period, Json configuration) {
        return null;
    }

    /**
     * Stop a task from running.
     */
    @Override
    public TaskManager stopTask(String id, String requesterName) {
        return null;
    }

    /**
     * Access the storage that this instance of TaskManager uses.
     * @return A TaskStateStorage object
     */
    @Override
    public TaskStateStorage storage() {
        return null;
    }
}
