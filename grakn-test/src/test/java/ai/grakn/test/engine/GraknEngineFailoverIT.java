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

package ai.grakn.test.engine;

import ai.grakn.client.TaskClient;
import ai.grakn.engine.TaskId;
import ai.grakn.engine.TaskStatus;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.engine.tasks.manager.ZookeeperConnection;
import ai.grakn.engine.tasks.mock.FailingMockTask;
import ai.grakn.engine.tasks.storage.TaskStateZookeeperStore;
import ai.grakn.exception.EngineStorageException;
import ai.grakn.generator.TaskStates.NewTask;
import ai.grakn.test.DistributionContext;
import ai.grakn.test.engine.tasks.BackgroundTaskTestUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.generator.Size;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.engine.TaskStatus.FAILED;
import static ai.grakn.engine.TaskStatus.STOPPED;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

@RunWith(JUnitQuickcheck.class)
public class GraknEngineFailoverIT {

    private static ZookeeperConnection connection;
    private static TaskStateStorage storage;

    @ClassRule
    public static DistributionContext engine1 = DistributionContext.startSingleQueueEngineProcess().port(4567);

    @ClassRule
    public static DistributionContext engine2 = DistributionContext.startSingleQueueEngineProcess().port(5678);

    @ClassRule
    public static DistributionContext engine3 = DistributionContext.startSingleQueueEngineProcess().port(6789);

    @BeforeClass
    public static void getStorage() {
        connection = new ZookeeperConnection();
        storage = new TaskStateZookeeperStore(connection);
    }

    @AfterClass
    public static void closeStorage() {
        connection.close();
    }

    @Property(trials=10)
    public void whenSubmittingTasksToOneEngine_TheyComplete(List<@NewTask TaskState> tasks1) throws Exception {
        // Create & Send tasks to rest api
        Set<TaskId> tasks = sendTasks(engine1.port(), tasks1);

        // Wait for those tasks to complete
        waitForStatus(tasks, COMPLETED, FAILED);

        // Assert the tasks have finished with the correct status depending on type
        assertTasksCompletedWithCorrectStatus(tasks);
    }


    @Property(trials=10)
    public void whenSubmittingTasksToTwoEngines_TheyComplete(
            List<@NewTask TaskState> tasks1, List<@NewTask TaskState> tasks2) throws Exception {
        // Create & Send tasks to rest api
        Set<TaskId> taskIds1 = sendTasks(engine1.port(), tasks1);
        Set<TaskId> taskIds2 = sendTasks(engine2.port(), tasks2);

        Set<TaskId> allTasks = new HashSet<>();
        allTasks.addAll(taskIds1);
        allTasks.addAll(taskIds2);

        // Wait for those tasks to complete
        waitForStatus(allTasks, COMPLETED, FAILED);

        // Assert the tasks have finished with the correct status depending on type
        assertTasksCompletedWithCorrectStatus(allTasks);
    }

    @Ignore //Failing randomly - may be a race condition
    @Property(trials=1)
    public void whenSubmittingTasksToOneEngineAndRandomlyKillingTheOthers_TheyComplete(
            @Size(min=1000, max=5000) List<@NewTask TaskState> tasks) throws Exception {

        Set<TaskId> taskIds = sendTasks(engine1.port(), tasks);

        // Randomly restart one of the other engines until all of the tasks are done
        int lowerBoundMs = 3000;
        Random random = new Random();
        List<DistributionContext> enginesToKill = ImmutableList.of(engine2, engine3);
        do{
            DistributionContext engineToKill = enginesToKill.get(random.nextInt(2));

            engineToKill.restart();

            int timeToSleep = random.nextInt(3000) + lowerBoundMs;
            Thread.sleep(timeToSleep);
        } while (!taskIds.stream().allMatch(GraknEngineFailoverIT::isDone));

        waitForStatus(taskIds, COMPLETED, FAILED);

        assertTasksCompletedWithCorrectStatus(taskIds);
    }

    private void assertTasksCompletedWithCorrectStatus(Set<TaskId> tasks) {
        tasks.stream().map(storage::getState).forEach(t -> {
            if(t.taskClass().equals(FailingMockTask.class)){
                assertThat(t.status(), equalTo(FAILED));
            } else {
                assertThat(t.status(), equalTo(COMPLETED));
            }
        });
    }


    private Set<TaskId> sendTasks(int port, List<TaskState> tasks) {
        TaskClient engineClient = TaskClient.of("localhost", port);

        return tasks.stream().map(t -> engineClient.sendTask(
                t.taskClass(),
                t.creator(),
                t.schedule().runAt(),
                t.schedule().interval().orElse(null),
                t.configuration())).collect(toSet());
    }

    private static void waitForStatus(Set<TaskId> taskIds, TaskStatus... status) {
        Set<TaskStatus> statusSet = Sets.newHashSet(status);
        taskIds.forEach(t -> BackgroundTaskTestUtils.waitForStatus(storage, t, statusSet));
    }

    private static boolean isDone(TaskId taskId){
        try {
            TaskStatus status = storage.getState(taskId).status();
            return status == FAILED || status == COMPLETED || status == STOPPED;
        } catch (EngineStorageException e){
            return false;
        }
    }
}