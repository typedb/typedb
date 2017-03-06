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

import ai.grakn.engine.tasks.TaskId;
import ai.grakn.engine.tasks.TaskSchedule;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.engine.tasks.config.ConfigHelper;
import ai.grakn.engine.tasks.manager.ZookeeperConnection;
import ai.grakn.engine.tasks.manager.multiqueue.MultiQueueTaskRunner;
import ai.grakn.engine.tasks.storage.TaskStateInMemoryStore;
import ai.grakn.engine.util.EngineID;
import ai.grakn.test.EngineContext;
import ai.grakn.test.engine.tasks.ShortExecutionTestTask;
import mjson.Json;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Set;

import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.engine.TaskStatus.SCHEDULED;
import static ai.grakn.engine.tasks.config.KafkaTerms.WORK_QUEUE_TOPIC;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.createTask;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.createTasks;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.waitForStatus;
import static java.util.Collections.singleton;
import static junit.framework.Assert.assertEquals;

public class MultiQueueTaskRunnerTest {

    private static ZookeeperConnection connection;

    private TaskStateStorage storage;
    private MultiQueueTaskRunner multiQueueTaskRunner;
    private Producer<TaskId, TaskState> producer;

    private Thread taskRunnerThread;

    @Rule
    public final EngineContext kafkaServer = EngineContext.startKafkaServer();

    @Before
    public void setup() throws Exception {
        connection = new ZookeeperConnection();

        producer = ConfigHelper.kafkaProducer();
        storage = new TaskStateInMemoryStore();

        multiQueueTaskRunner = new MultiQueueTaskRunner(EngineID.of("me"), storage, connection);
        taskRunnerThread = new Thread(multiQueueTaskRunner);
        taskRunnerThread.start();
    }

    @After
    public void tearDown() throws Exception {
        producer.close();

        multiQueueTaskRunner.close();
        taskRunnerThread.join();

        connection.close();
    }

    @Test
    public void testSendReceive() throws Exception {
        ShortExecutionTestTask.startedCounter.set(0);
        ShortExecutionTestTask.resumedCounter.set(0);

        Set<TaskState> tasks = createTasks(5, SCHEDULED);
        tasks.forEach(storage::newState);
        sendTasksToWorkQueue(tasks);
        waitForStatus(storage, tasks, COMPLETED);

        assertEquals(5, ShortExecutionTestTask.startedCounter.get());
    }

    @Test
    public void testSendDuplicate() throws Exception {
        ShortExecutionTestTask.startedCounter.set(0);
        ShortExecutionTestTask.resumedCounter.set(0);

        Set<TaskState> tasks = createTasks(5, SCHEDULED);
        tasks.forEach(storage::newState);
        sendTasksToWorkQueue(tasks);
        sendTasksToWorkQueue(tasks);

        waitForStatus(storage, tasks, COMPLETED);
        assertEquals(5, ShortExecutionTestTask.startedCounter.get());
    }

    @Test
    public void testSendWithCheckpoint() {
        ShortExecutionTestTask.startedCounter.set(0);
        ShortExecutionTestTask.resumedCounter.set(0);

        TaskState task = createTask(SCHEDULED, TaskSchedule.now(), Json.object());
        task.checkpoint("");
        storage.newState(task);
        sendTasksToWorkQueue(singleton(task));

        waitForStatus(storage, singleton(task), COMPLETED);

        // Task should be resumed, not started
        // This is because it was sent to the work queue with a non-null checkpoint
        assertEquals(1, ShortExecutionTestTask.resumedCounter.get());
        assertEquals(0, ShortExecutionTestTask.startedCounter.get());
    }

    private void sendTasksToWorkQueue(Set<TaskState> tasks) {
        tasks.forEach(t -> producer.send(new ProducerRecord<>(WORK_QUEUE_TOPIC, t.getId(), t)));
        producer.flush();
    }
}
