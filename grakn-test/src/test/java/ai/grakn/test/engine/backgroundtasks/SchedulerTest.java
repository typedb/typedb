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

import ai.grakn.engine.backgroundtasks.TaskState;
import ai.grakn.engine.backgroundtasks.TaskStateStorage;
import ai.grakn.engine.backgroundtasks.config.ConfigHelper;
import ai.grakn.engine.backgroundtasks.distributed.Scheduler;
import ai.grakn.engine.backgroundtasks.taskstatestorage.TaskStateInMemoryStore;
import ai.grakn.test.EngineContext;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Set;

import static ai.grakn.engine.backgroundtasks.TaskStatus.CREATED;
import static ai.grakn.engine.backgroundtasks.TaskStatus.SCHEDULED;
import static ai.grakn.engine.backgroundtasks.config.KafkaTerms.NEW_TASKS_TOPIC;
import static ai.grakn.test.engine.backgroundtasks.BackgroundTaskTestUtils.createTasks;
import static ai.grakn.test.engine.backgroundtasks.BackgroundTaskTestUtils.waitForStatus;

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

    private Thread schedulerThread;

    @Rule
    public final EngineContext kafkaServer = EngineContext.startKafkaServer();

    @Before
    public void start() throws Exception {
        storage = new TaskStateInMemoryStore();
        scheduler = new Scheduler(storage);

        schedulerThread = new Thread(scheduler);
        schedulerThread.start();

        producer = ConfigHelper.kafkaProducer();
    }

    @After
    public void stop() throws Exception {
        producer.close();

        scheduler.close();
        schedulerThread.join();
    }

    @Test
    public void immediateNonRecurringScheduled() {
        Set<TaskState> tasks = createTasks(storage, 5, CREATED);
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
        Set<TaskState> tasks = createTasks(storage, 5, CREATED);
        sendTasksToNewTasksQueue(tasks);
        waitForStatus(storage, tasks, SCHEDULED);

        // Kill the scheduler
        producer.close();
        scheduler.close();
        schedulerThread.join();
        producer = ConfigHelper.kafkaProducer();

        // Schedule 5 more tasks
        tasks = createTasks(storage, 5, CREATED);
        sendTasksToNewTasksQueue(tasks);

        // Restart the scheduler
        scheduler = new Scheduler(storage);

        schedulerThread = new Thread(scheduler);
        schedulerThread.start();

        // Wait for new tasks to complete
        waitForStatus(storage, tasks, SCHEDULED);

        tasks.stream().map(TaskState::getId)
                .map(storage::getState)
                .map(TaskState::status)
                .allMatch(t -> t.equals(SCHEDULED));
    }

    private void sendTasksToNewTasksQueue(Set<TaskState> tasks) {
        tasks.forEach(t -> producer.send(new ProducerRecord<>(NEW_TASKS_TOPIC, t.getId(), t.configuration().toString())));
        producer.flush();
    }
}
