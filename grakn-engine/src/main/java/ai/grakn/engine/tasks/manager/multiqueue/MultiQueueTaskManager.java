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
import ai.grakn.engine.cache.EngineCacheDistributed;
import ai.grakn.engine.cache.EngineCacheProvider;
import ai.grakn.engine.tasks.TaskManager;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.engine.tasks.config.ConfigHelper;
import ai.grakn.engine.tasks.manager.ZookeeperConnection;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.util.EngineID;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static ai.grakn.engine.tasks.config.KafkaTerms.NEW_TASKS_TOPIC;
import static ai.grakn.engine.util.ExceptionWrapper.noThrow;
import static java.lang.String.format;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;

/**
 * Class to manage tasks distributed using Kafka.
 *
 * @author Denis Lobanov
 * This class begins the TaskRunner instance that will be running on this machine.
 */
//TODO: Kill this
public final class MultiQueueTaskManager implements TaskManager {

    private final static Logger LOG = LoggerFactory.getLogger(MultiQueueTaskManager.class);
    private final static GraknEngineConfig properties = GraknEngineConfig.getInstance();

    private final Producer<TaskId, TaskState> producer;
    private final SchedulerElector elector;
    private final ZookeeperConnection zookeeper;
    private final TaskStateStorage storage;

    private static final String TASKRUNNER_THREAD_NAME = "taskrunner-";
    private final EngineID engineId;
    private MultiQueueTaskRunner multiQueueTaskRunner;
    private Thread taskRunnerThread;

    public MultiQueueTaskManager(EngineID engineId) {
        this.engineId = engineId;
        this.zookeeper = new ZookeeperConnection();
        this.storage = chooseStorage(properties, zookeeper);

        // run the TaskRunner in a thread
        startTaskRunner();

        // Elect the scheduler or add yourself to the scheduler pool
        elector = new SchedulerElector(storage, zookeeper);

        this.producer = ConfigHelper.kafkaProducer();
        EngineCacheProvider.init(EngineCacheDistributed.init(zookeeper));
    }

    @Override
    public void close() {
        LOG.debug("Closing TaskManager");

        // close kafka producer
        noThrow(producer::close, "Error shutting down producer in TaskManager");

        // remove this engine from leadership election
        noThrow(elector::stop, "Error stopping Scheduler elector from TaskManager");

        // close task runner
        noThrow(multiQueueTaskRunner::close, "Error shutting down TaskRunner");
        noThrow(taskRunnerThread::join, "Error waiting for TaskRunner to close");

        // stop zookeeper connection
        noThrow(zookeeper::close, "Error waiting for zookeeper connection to close");

        EngineCacheProvider.clearCache();

        LOG.debug("TaskManager closed");
    }

    @Override
    public void addTask(TaskState taskState){
        producer.send(new ProducerRecord<>(NEW_TASKS_TOPIC, taskState.getId(), taskState));
        producer.flush();
    }

    @Override
    public void stopTask(TaskId id) {
        throw new UnsupportedOperationException(this.getClass().getName() + " currently doesn't support stopping tasks");
    }

    @Override
    public TaskStateStorage storage() {
        return storage;
    }

    /**
     * Start a new instance of TaskRunner in a thread.
     *
     * We want to revive the TaskRunner if an unhandled exception is thrown. To handle:
     *        It is instantiated with a TaskRunnerResurrection exception
     *        handler that will restart the task runner if any unchecked exception is thrown.
     */
    private void startTaskRunner(){
        multiQueueTaskRunner = new MultiQueueTaskRunner(engineId, storage, zookeeper);
        taskRunnerThread = new Thread(multiQueueTaskRunner, TASKRUNNER_THREAD_NAME + multiQueueTaskRunner.hashCode());
        taskRunnerThread.setUncaughtExceptionHandler(new TaskRunnerResurrection());
        taskRunnerThread.start();
    }

    /**
     * Implementation of UncaughtExceptionHandler that will restart the TaskRunner in a new thread
     * if it throws any unchecked exception
     *
     * @author alexandraorth
     */
    private class TaskRunnerResurrection implements Thread.UncaughtExceptionHandler {

        public void uncaughtException(Thread paramThread, Throwable paramThrowable) {
            LOG.debug(format("TaskRunner [%s] threw an exception. Will attempt to close and reopen. Exception is: %n [%s]",
                    paramThread.getName(), getFullStackTrace(paramThrowable)));

            noThrow(multiQueueTaskRunner::close, "Error shutting down TaskRunner");
            // no need to call taskRunnerThread.join() here - recursive wait, are still in thread

            LOG.debug("TaskRunner closed.");

            startTaskRunner();

            LOG.debug("Re-instantiation of TaskRunner completed.");
        }
    }
}
