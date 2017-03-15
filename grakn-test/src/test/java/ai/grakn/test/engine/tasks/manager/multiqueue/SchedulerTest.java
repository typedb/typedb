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

package ai.grakn.test.engine.tasks.manager.multiqueue;

import ai.grakn.engine.TaskStatus;
import ai.grakn.engine.TaskId;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.engine.tasks.config.ConfigHelper;
import ai.grakn.engine.tasks.manager.ZookeeperConnection;
import ai.grakn.engine.tasks.manager.multiqueue.Scheduler;
import ai.grakn.engine.tasks.storage.TaskStateInMemoryStore;
import ai.grakn.test.EngineContext;
import ai.grakn.engine.tasks.mock.ShortExecutionMockTask;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import static ai.grakn.engine.TaskStatus.SCHEDULED;
import static ai.grakn.engine.tasks.TaskSchedule.recurring;
import static ai.grakn.engine.tasks.config.KafkaTerms.NEW_TASKS_TOPIC;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.createTask;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.createTasks;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.waitForStatus;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toList;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Test the Scheduler with an In Memory state storage. Kafka needs to be running.
 * Run the Scheduler in another thread, submit some tasks to the correct kafka queue&
 * wait for them to be scheduled, then close the thread.
 *
 * @author alexandraorth
 */
public class SchedulerTest {

    private static TaskStateStorage storage;
    private static Scheduler scheduler;
    private static Producer<TaskId, TaskState> producer;
    private static ZookeeperConnection connection;
    private static Thread schedulerThread;

    @ClassRule
    public static final EngineContext kafkaServer = EngineContext.startKafkaServer();

    @BeforeClass
    public static void start() throws Exception {
        storage = new TaskStateInMemoryStore();
        connection = new ZookeeperConnection();
        startScheduler();
        producer = ConfigHelper.kafkaProducer();
    }

    @AfterClass
    public static void stop() throws Exception {
        producer.close();
        connection.close();
        stopScheduler();
    }

    @Test
    public void immediateNonRecurringScheduled() {
        Set<TaskState> tasks = createTasks(5);
        sendTasksToNewTasksQueue(tasks);
        waitForStatus(storage, tasks, SCHEDULED);

        List<TaskStatus> statuses = tasks.stream().map(TaskState::getId).map(storage::getState).map(TaskState::status).collect(toList());
        assertThat(statuses, everyItem(is(SCHEDULED)));
    }

    @Test
    public void schedulerPicksUpFromLastOffset() throws InterruptedException {
        // Schedule 5 tasks
        Set<TaskState> tasks = createTasks(5);
        sendTasksToNewTasksQueue(tasks);
        waitForStatus(storage, tasks, SCHEDULED);

        // Kill the scheduler
        producer.close();
        stopScheduler();
        producer = ConfigHelper.kafkaProducer();

        // Schedule 5 more tasks
        tasks = createTasks(5);
        sendTasksToNewTasksQueue(tasks);

        // Restart the scheduler
        startScheduler();

        // Wait for new tasks to complete
        waitForStatus(storage, tasks, SCHEDULED);

        List<TaskStatus> statuses = tasks.stream().map(TaskState::getId).map(storage::getState).map(TaskState::status).collect(toList());
        assertThat(statuses, everyItem(is(SCHEDULED)));
    }

    @Test
    public void testRecurringTasksRestarted() throws InterruptedException {
        // close the scheduler thread
        stopScheduler();

        // persist a recurring task
        TaskState recurring = createTask(ShortExecutionMockTask.class, recurring(Duration.ofSeconds(10)));
        System.out.println("recurring task " + recurring.getId());
        storage.newState(recurring);

        // check the task actually exists and is recurring
        TaskState recurringPersisted = storage.getState(recurring.getId());
        assertNotNull(recurringPersisted);
        assertTrue(recurringPersisted.schedule().isRecurring());

        // Restart the scheduler
        startScheduler();

        // Check that the recurring task has been scheduled
        waitForStatus(storage, singleton(recurring), SCHEDULED);

        assertEquals(storage.getState(recurring.getId()).status(), SCHEDULED);
    }

    private static void stopScheduler() throws InterruptedException {
        scheduler.close();
        schedulerThread.join();
    }

    private static void startScheduler(){
        // Restart the scheduler
        scheduler = new Scheduler(storage, connection);

        schedulerThread = new Thread(scheduler);
        schedulerThread.start();
    }

    private void sendTasksToNewTasksQueue(Set<TaskState> tasks) {
        System.out.println("Producer sending to new tasks queue: " + tasks);
        tasks.forEach(t -> producer.send(new ProducerRecord<>(NEW_TASKS_TOPIC, t.getId(), t)));
        producer.flush();
    }
}
