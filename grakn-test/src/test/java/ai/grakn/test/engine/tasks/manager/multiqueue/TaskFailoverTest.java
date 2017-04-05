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

package ai.grakn.test.engine.tasks.manager.multiqueue;

import ai.grakn.engine.TaskId;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.manager.ZookeeperConnection;
import ai.grakn.engine.tasks.manager.singlequeue.TaskFailover;
import ai.grakn.engine.tasks.storage.TaskStateInMemoryStore;
import ai.grakn.engine.util.EngineID;
import ai.grakn.test.EngineContext;
import com.google.common.collect.Sets;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.Producer;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;

import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.engine.TaskStatus.FAILED;
import static ai.grakn.engine.TaskStatus.RUNNING;
import static ai.grakn.engine.TaskStatus.STOPPED;
import static ai.grakn.engine.tasks.config.ZookeeperPaths.SINGLE_ENGINE_WATCH_PATH;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.createRunningTasks;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.createTask;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.waitForStatus;
import static java.lang.String.format;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@Ignore
public class TaskFailoverTest {

    private static ZookeeperConnection connection;
    private static TaskFailover taskFailover;
    private static Producer<TaskId, TaskState> producer;
    private static TaskStateInMemoryStore storage;

    @Rule
    public final EngineContext kafkaServer = EngineContext.startKafkaServer();

    @Before
    public void setup() throws Exception {
        storage = new TaskStateInMemoryStore();

        connection = new ZookeeperConnection();

        producer = mock(Producer.class);
        taskFailover = new TaskFailover(connection.connection(), storage, producer);
    }

    @After
    public void teardown() throws Exception {
        taskFailover.close();
        connection.close();
    }

    @Test
    public void whenAnEngineWithRUNNINGTasksFails_ThatTaskAddedToWorkQueue() throws Exception {
        EngineID fakeEngineID = EngineID.of(UUID.randomUUID().toString());
        registerFakeEngine(fakeEngineID);

        // Add some tasks to a fake task runner watch and storage, marked as running
        Set<TaskState> tasks = createRunningTasks(5, fakeEngineID);
        tasks.forEach(storage::newState);

        // Mock killing that engine in ZK
        killFakeEngine(fakeEngineID);

        // Wait for those tasks to show up in the work queue
        waitForStatus(storage, tasks, RUNNING);

        // Check they are all running in storage
        assertTrue(tasks.stream().map(TaskState::getId)
                .map(storage::getState)
                .map(TaskState::status)
                .allMatch(t -> t.equals(RUNNING)));

        Set<TaskId> taskIds = tasks.stream().map(TaskState::getId).collect(Collectors.toSet());

        // Verify tasks was sent to the queue
        verify(producer, timeout(5000).times(5))
                .send(argThat(argument -> taskIds.contains(argument.key())));
    }

    @Test
    public void whenAnEngineWithNonRUNNINGTasksFails_ThoseTasksNotAddedToWorkQueue() throws Exception {
        // When a task has been added to the task runner watch, but is not marked
        // as running at the time of failure, it should not be added to the work queue
        EngineID fakeEngineID = EngineID.of(UUID.randomUUID().toString());
        registerFakeEngine(fakeEngineID);

        // Add a task in each state (SCHEDULED, COMPLETED, STOPPED, FAILED, RUNNING) to fake task runner watch
        TaskState running = createTask().markRunning(fakeEngineID);
        TaskState stopped = createTask().markStopped();
        TaskState failed = createTask().markFailed(new IOException());
        TaskState completed = createTask().markCompleted();

        Set<TaskState> tasks = Sets.newHashSet(running, stopped, failed, completed);
        tasks.forEach(storage::newState);

        // Mock killing that engine
        killFakeEngine(fakeEngineID);

        // Make sure only the running task ends up in the work queue
        waitForStatus(storage, Sets.newHashSet(running), RUNNING);

        // the task that was in the middle of running should be marked as running
        assertEquals(RUNNING, storage.getState(running.getId()).status());

        // the three other tasks should keep their state in the storage
        assertEquals(COMPLETED, storage.getState(completed.getId()).status());
        assertEquals(STOPPED, storage.getState(stopped.getId()).status());
        assertEquals(FAILED, storage.getState(failed.getId()).status());

        // Verify a task was sent to the queue only once
        verify(producer, timeout(5000).times(1))
                .send(argThat(argument -> argument.key().equals(running.getId())));
    }

    private void registerFakeEngine(EngineID id) throws Exception{
        if (connection.connection().checkExists().forPath(format(SINGLE_ENGINE_WATCH_PATH, id.value())) == null) {
            connection.connection().create()
                    .creatingParentContainersIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(format(SINGLE_ENGINE_WATCH_PATH, id.value()));
        }
        assertNotNull(connection.connection().checkExists().forPath(format(SINGLE_ENGINE_WATCH_PATH, id.value())));
    }

    private void killFakeEngine(EngineID id) throws Exception {
        connection.connection().delete().forPath(format(SINGLE_ENGINE_WATCH_PATH, id.value()));
        assertNull(connection.connection().checkExists().forPath(format(SINGLE_ENGINE_WATCH_PATH, id.value())));
    }
}
