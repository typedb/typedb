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

import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.manager.multiqueue.TaskFailover;
import ai.grakn.engine.tasks.manager.ZookeeperConnection;
import ai.grakn.engine.tasks.storage.TaskStateInMemoryStore;
import ai.grakn.test.EngineContext;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.google.common.collect.Sets;
import mjson.Json;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.zookeeper.CreateMode;
import org.json.JSONArray;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.engine.TaskStatus.FAILED;
import static ai.grakn.engine.TaskStatus.RUNNING;
import static ai.grakn.engine.TaskStatus.SCHEDULED;
import static ai.grakn.engine.TaskStatus.STOPPED;
import static ai.grakn.engine.tasks.config.ConfigHelper.client;
import static ai.grakn.engine.tasks.config.ZookeeperPaths.RUNNERS_STATE;
import static ai.grakn.engine.tasks.config.ZookeeperPaths.RUNNERS_WATCH;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.createTask;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.createTasks;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.waitForStatus;
import static java.util.Collections.singleton;
import static junit.framework.TestCase.assertEquals;

public class TaskFailoverTest {

    private static ZookeeperConnection connection;
    private static TaskFailover taskFailover;
    private static TreeCache treeCache;
    private static TaskStateInMemoryStore storage;

    private static Thread failoverThread;

    @ClassRule
    public static final EngineContext kafkaServer = EngineContext.startKafkaServer();

    @BeforeClass
    public static void setup() throws Exception {
        ((Logger) org.slf4j.LoggerFactory.getLogger(TaskFailover.class)).setLevel(Level.DEBUG);

        storage = new TaskStateInMemoryStore();

        connection = new ZookeeperConnection(client());

        CountDownLatch failoverStartup = new CountDownLatch(1);
        failoverThread = new Thread(() -> {
            try {
                treeCache = new TreeCache(connection.connection(), RUNNERS_WATCH);
                taskFailover = new TaskFailover(connection.connection(), treeCache, storage);
                treeCache.getListenable().addListener(taskFailover);
                treeCache.start();
                failoverStartup.countDown();
            } catch (Exception e){
                throw new RuntimeException(e);
            }
        });

        failoverThread.start();
        failoverStartup.await();
    }

    @AfterClass
    public static void teardown() throws Exception {
        taskFailover.close();
        treeCache.close();
        connection.close();

        failoverThread.interrupt();
        failoverThread.join();
    }

    @Test
    public void runningTasksWhenEngineFailAddedToWorkQueue() throws Exception {
        String fakeEngineID = UUID.randomUUID().toString();
        registerFakeEngine(fakeEngineID);

        // Add some tasks to a fake task runner watch and storage, marked as running
        Set<TaskState> tasks = createTasks(5, RUNNING);
        tasks.forEach(storage::newState);
        registerTasksInZKLikeTaskRunnerWould(fakeEngineID, tasks);

        // Mock killing that engine in ZK
        killFakeEngine(fakeEngineID);

        // Wait for those tasks to show up in the work queue
        waitForStatus(storage, tasks, SCHEDULED);

        // Check they are all scheduled in storage
        tasks.stream().map(TaskState::getId)
                .map(storage::getState)
                .map(TaskState::status)
                .allMatch(t -> t.equals(SCHEDULED));
    }

    @Test
    public void nonRUNNINGTasksOnTaskRunnerPathNotAddedToWorkQueue() throws Exception {
        // When a task has been added to the task runner watch, but is not marked
        // as running at the time of failure, it should not be added to the work queue
        String fakeEngineID = UUID.randomUUID().toString();
        registerFakeEngine(fakeEngineID);

        // Add a task in each state (SCHEDULED, COMPLETED, STOPPED, FAILED, RUNNING) to fake task runner watch
        TaskState scheduled = createTask(0, SCHEDULED, false, 0);
        TaskState running = createTask(1, RUNNING, false, 0);
        TaskState stopped = createTask(2, STOPPED, false, 0);
        TaskState failed = createTask(3, FAILED, false, 0);
        TaskState completed = createTask(4, COMPLETED, false, 0);

        Set<TaskState> tasks = Sets.newHashSet(scheduled, running, stopped, failed, completed);
        tasks.forEach(storage::newState);
        registerTasksInZKLikeTaskRunnerWould(fakeEngineID, tasks);

        // Mock killing that engine
        killFakeEngine(fakeEngineID);

        // Make sure only the running task ends up in the work queue
        waitForStatus(storage, Sets.newHashSet(running, scheduled), SCHEDULED);

        // the task that was in the middle of running should be marked as scheduled
        assertEquals(SCHEDULED, storage.getState(running.getId()).status());

        // the task that was scheduled should still be marked as scheduled and in the work queue
        assertEquals(SCHEDULED, storage.getState(scheduled.getId()).status());

        // the three other tasks should keep their state in the storage
        assertEquals(COMPLETED, storage.getState(completed.getId()).status());
        assertEquals(STOPPED, storage.getState(stopped.getId()).status());
        assertEquals(FAILED, storage.getState(failed.getId()).status());
    }

    @Test
    public void failoverTasksRestartedFromCorrectCheckpoints() throws Exception {
        // On failover, tasks should be restarted from where they left off execution
        String fakeEngineID = UUID.randomUUID().toString();
        registerFakeEngine(fakeEngineID);

        // Add some tasks to a storage with a specific checkpoint
        Json configuration = Json.object("configuration", true);
        Json checkpoint = Json.object("configuration", false);

        TaskState running = createTask(1, RUNNING, false, 0);
        running.configuration(configuration);
        running.checkpoint(checkpoint.toString());
        storage.newState(running);

        // Add them to the task runner watch
        registerTasksInZKLikeTaskRunnerWould(fakeEngineID, singleton(running));

        // Mock killing that engine
        killFakeEngine(fakeEngineID);

        // Check the tasks end up in the work queue with the correct checkpoints
        waitForStatus(storage, singleton(running), SCHEDULED);

        assertEquals(SCHEDULED, storage.getState(running.getId()).status());
        assertEquals(checkpoint.toString(), storage.getState(running.getId()).checkpoint());
    }

    private void registerFakeEngine(String id) throws Exception{
        if (connection.connection().checkExists().forPath(RUNNERS_WATCH + "/" + id) == null) {
            connection.connection().create()
                    .creatingParentContainersIfNeeded()
                    .withMode(CreateMode.EPHEMERAL).forPath(RUNNERS_WATCH + "/" + id);
        }

        if (connection.connection().checkExists().forPath(RUNNERS_STATE + "/" + id) == null) {
            connection.connection().create()
                    .creatingParentContainersIfNeeded()
                    .forPath(RUNNERS_STATE + "/" + id);
        }
    }

    private void registerTasksInZKLikeTaskRunnerWould(String id, Set<TaskState> tasks) throws Exception{
        JSONArray out = new JSONArray();
        tasks.stream().map(TaskState::getId).forEach(out::put);

        connection.connection().setData().forPath(RUNNERS_STATE + "/" + id, out.toString().getBytes(StandardCharsets.UTF_8));
    }

    private void killFakeEngine(String id) throws Exception {
        connection.connection().delete().forPath(RUNNERS_WATCH + "/" + id);
    }
}
