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

package ai.grakn.test.engine.backgroundtasks;

import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.engine.tasks.config.ConfigHelper;
import ai.grakn.engine.tasks.manager.multiqueue.Scheduler;
import ai.grakn.engine.tasks.manager.multiqueue.MultiQueueTaskRunner;
import ai.grakn.engine.tasks.manager.ZookeeperConnection;
import ai.grakn.engine.tasks.storage.TaskStateGraphStore;
import ai.grakn.engine.tasks.storage.TaskStateInMemoryStore;
import ai.grakn.engine.util.ExceptionWrapper;
import ai.grakn.test.EngineContext;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Set;

import static ai.grakn.engine.TaskStatus.CREATED;
import static ai.grakn.engine.TaskStatus.SCHEDULED;
import static ai.grakn.engine.tasks.config.KafkaTerms.NEW_TASKS_TOPIC;
import static ai.grakn.test.engine.backgroundtasks.BackgroundTaskTestUtils.createTask;
import static ai.grakn.test.engine.backgroundtasks.BackgroundTaskTestUtils.createTasks;
import static ai.grakn.test.engine.backgroundtasks.BackgroundTaskTestUtils.waitForStatus;
import static java.util.Collections.singleton;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertEquals;

/**
 * Test the Scheduler with an In Memory state storage. Kafka needs to be running.
 * Run the Scheduler in another thread, submit some tasks to the correct kafka queue&
 * wait for them to be scheduled, then close the thread.
 *
 * @author alexandraorth
 */
public class SchedulerTest {

    private TaskStateStorage storage;
    private Scheduler scheduler;
    private KafkaProducer<String, String> producer;

    private ZookeeperConnection connection;
    private Thread schedulerThread;

    @ClassRule
    public static final EngineContext kafkaServer = EngineContext.startKafkaServer();

    @Before
    public void start() throws Exception {
        ((Logger) org.slf4j.LoggerFactory.getLogger(ExceptionWrapper.class)).setLevel(Level.DEBUG);
        ((Logger) org.slf4j.LoggerFactory.getLogger(Scheduler.class)).setLevel(Level.DEBUG);
        ((Logger) org.slf4j.LoggerFactory.getLogger(MultiQueueTaskRunner.class)).setLevel(Level.DEBUG);
        ((Logger) org.slf4j.LoggerFactory.getLogger(TaskStateGraphStore.class)).setLevel(Level.DEBUG);

        storage = new TaskStateInMemoryStore();
        connection = new ZookeeperConnection();
        startScheduler();
        producer = ConfigHelper.kafkaProducer();
    }

    @After
    public void stop() throws Exception {
        producer.close();
        connection.close();
        stopScheduler();
    }

    @Test
    public void immediateNonRecurringScheduled() {
        Set<TaskState> tasks = createTasks(5, CREATED);
        sendTasksToNewTasksQueue(tasks);
        waitForStatus(storage, tasks, SCHEDULED);

        tasks.stream().map(TaskState::getId)
                .map(storage::getState)
                .map(TaskState::status)
                .allMatch(t -> t.equals(SCHEDULED));
    }

    @Test
    public void schedulerPicksUpFromLastOffset() throws InterruptedException {
        // Schedule 5 tasks
        Set<TaskState> tasks = createTasks(5, CREATED);
        sendTasksToNewTasksQueue(tasks);
        waitForStatus(storage, tasks, SCHEDULED);

        // Kill the scheduler
        producer.close();
        stopScheduler();
        producer = ConfigHelper.kafkaProducer();

        // Schedule 5 more tasks
        tasks = createTasks(5, CREATED);
        sendTasksToNewTasksQueue(tasks);

        // Restart the scheduler
        startScheduler();

        // Wait for new tasks to complete
        waitForStatus(storage, tasks, SCHEDULED);

        tasks.stream().map(TaskState::getId)
                .map(storage::getState)
                .map(TaskState::status)
                .allMatch(t -> t.equals(SCHEDULED));
    }

    @Test
    public void testRecurringTasksRestarted() throws InterruptedException {
        // close the scheduler thread
        stopScheduler();

        // persist a recurring task
        TaskState recurring = createTask(1, CREATED, true, 10000);
        System.out.println("recurring task " + recurring.getId());
        storage.newState(recurring);

        // check the task actually exists and is recurring
        TaskState recurringPersisted = storage.getState(recurring.getId());
        assertNotNull(recurringPersisted);
        assertTrue(recurringPersisted.isRecurring());

        // Restart the scheduler
        startScheduler();

        // Check that the recurring task has been scheduled
        waitForStatus(storage, singleton(recurring), SCHEDULED);

        assertEquals(storage.getState(recurring.getId()).status(), SCHEDULED);
    }

    private void stopScheduler() throws InterruptedException {
        scheduler.close();
        schedulerThread.join();
    }

    private void startScheduler(){
        // Restart the scheduler
        scheduler = new Scheduler(storage, connection);

        schedulerThread = new Thread(scheduler);
        schedulerThread.start();
    }

    private void sendTasksToNewTasksQueue(Set<TaskState> tasks) {
        System.out.println("Producer sending to new tasks queue: " + tasks);
        tasks.forEach(t -> producer.send(new ProducerRecord<>(NEW_TASKS_TOPIC, t.getId(), TaskState.serialize(t))));
        producer.flush();
    }
}
